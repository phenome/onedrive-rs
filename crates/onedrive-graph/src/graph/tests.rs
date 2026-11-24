use super::*;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn initial_delta_targets_the_personal_drive() {
    let server = MockServer::start().await;
    let graph = mock_graph(&server);
    let link = format!("{}/v1.0/me/drive/root/delta?token=a%2Bb%2f", server.uri());
    Mock::given(method("GET"))
        .and(path("/v1.0/me/drive/root/delta"))
        .and(query_param("$top", "1000"))
        .and(header("authorization", "Bearer synthetic-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"id":"root", "name":"root", "folder":{}, "root":{}}],
            "@odata.deltaLink":link
        })))
        .expect(1)
        .mount(&server)
        .await;
    let page = graph.delta(DeltaRequest::Initial).await.unwrap();
    assert!(matches!(page.cursor, DeltaCursor::Complete(url) if url == link));
    assert!(matches!(&page.changes[..], [DeltaChange::Upsert(item)] if item.item_id == "root"));
}

pub(super) fn mock_graph(server: &MockServer) -> GraphDrive {
    GraphDrive::with_base(
        Arc::new(Auth::synthetic()),
        Url::parse(&format!("{}/v1.0", server.uri())).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn graph_range_populates_engine_sparse_cache() {
    use onedrive_core::{Engine, SystemClock};

    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("metadata.db");
    let cache = temp.path().join("cache");
    let bytes: Vec<_> = (0..65_536).map(|offset| (offset % 251) as u8).collect();

    Mock::given(method("GET"))
        .and(path("/v1.0/me/drive/root/delta"))
        .and(query_param("$top", "1000"))
        .and(header("authorization", "Bearer synthetic-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [
                {"id": "root", "name": "root", "folder": {}, "root": {}},
                {"id": "file", "name": "photo.png", "file": {}, "size": 131072,
                 "parentReference": {"id": "root"}, "eTag": "item-1", "cTag": "content-1"}
            ],
            "@odata.deltaLink": format!("{}/v1.0/me/drive/root/delta?token=complete", server.uri())
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1.0/me/drive/items/file"))
        .and(header("authorization", "Bearer synthetic-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "@microsoft.graph.downloadUrl": format!("{}/content?sig=temporary", cdn.uri())
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/content"))
        .and(query_param("sig", "temporary"))
        .and(header("range", "bytes=0-65535"))
        .and(anonymous)
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-65535/131072")
                .set_body_bytes(bytes.clone()),
        )
        .expect(1)
        .mount(&cdn)
        .await;

    let open = || {
        Engine::open_with_cache(
            &database,
            &cache,
            Arc::new(mock_graph(&server)),
            Arc::new(SystemClock),
        )
        .unwrap()
    };
    let engine = open();
    engine.sync().await.unwrap();
    let inode = engine.lookup("root", "photo.png").unwrap().unwrap().inode;
    assert_eq!(engine.read(inode, 0, 32768).await.unwrap(), bytes[..32768]);
    drop(engine);

    let engine = open();
    assert_eq!(
        engine.read(inode, 40000, 123).await.unwrap(),
        bytes[40000..40123]
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(cdn.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn delta_preserves_opaque_links_and_maps_expired_cursors() {
    let server = MockServer::start().await;
    let graph = mock_graph(&server);
    let query = "token=a%2Bb%2f&key=x+y&key=y";
    let next = format!("{}/v1.0/me/drive/root/delta?{query}", server.uri());
    let complete = format!("{}/v1.0/me/drive/root/delta?token=final%2f", server.uri());
    Mock::given(query_param("$top", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [
                {"id":"deleted","deleted":{}},
                {"id":"file","parentReference":{"id":"folder","path":"stale"},
                 "size":42,"eTag":"etag","cTag":"ctag","file":{"hashes":{"quickXorHash":"hash"}},
                 "fileSystemInfo":{"lastModifiedDateTime":"2026-01-01T00:00:00.123Z"}}
            ],
            "@odata.nextLink":next
        })))
        .mount(&server)
        .await;
    Mock::given(move |request: &wiremock::Request| request.url.query() == Some(query))
        .and(header("authorization", "Bearer synthetic-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [], "@odata.deltaLink":complete
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(query_param("token", "final/"))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({"error":{"code":"resyncRequired"}})),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(query_param("token", "gone"))
        .respond_with(ResponseTemplate::new(410))
        .expect(1)
        .mount(&server)
        .await;

    let page = graph.delta(DeltaRequest::Initial).await.unwrap();
    assert!(
        matches!(&page.changes[..], [DeltaChange::Delete(id), DeltaChange::Upsert(item)]
        if id == "deleted" && *item == RemoteItem {
            item_id: "file".into(), parent_id: Some("folder".into()), name: None,
            kind: ItemKind::File, size: 42, mtime: 1_767_225_600, etag: Some("etag".into()),
            ctag: Some("ctag".into()), quickxor_hash: Some("hash".into()),
        })
    );
    let DeltaCursor::Next(link) = page.cursor else {
        panic!("missing next link")
    };
    assert_eq!(link, next);
    let page = graph.delta(DeltaRequest::Link(link)).await.unwrap();
    let DeltaCursor::Complete(link) = page.cursor else {
        panic!("missing delta link")
    };
    assert_eq!(link, complete);
    for link in [
        link,
        format!("{}/v1.0/me/drive/root/delta?token=gone", server.uri()),
    ] {
        assert!(matches!(
            graph.delta(DeltaRequest::Link(link)).await,
            Err(CloudError::ResyncRequired)
        ));
    }
}

pub(super) fn anonymous(request: &wiremock::Request) -> bool {
    !request.headers.contains_key(header::AUTHORIZATION)
}

#[tokio::test]
async fn folder_thumbnails_follow_pages_and_download_without_bearer() {
    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    let graph = mock_graph(&server);
    let thumbnail = format!("{}/image.jpg?sig=temporary", cdn.uri());
    Mock::given(method("GET"))
        .and(path("/v1.0/me/drive/items/folder/children"))
        .and(query_param("$expand", "thumbnails"))
        .and(header("authorization", "Bearer synthetic-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value":[{"id":"plain"},{"id":"small","thumbnails":[{"small":{"url":"unused"}}]}],
            "@odata.nextLink":format!("{}/page?exact=a%2f", server.uri())
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/page"))
        .and(|request: &wiremock::Request| request.url.query() == Some("exact=a%2f"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value":[{"id":"photo","thumbnails":[{},{"large":{"url":thumbnail}}]}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/image.jpg"))
        .and(query_param("sig", "temporary"))
        .and(anonymous)
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![255, 216, 255, 217]))
        .expect(1)
        .mount(&cdn)
        .await;
    let thumbnails = graph.list_thumbnails("folder").await.unwrap();
    assert_eq!(thumbnails.len(), 1);
    assert_eq!(thumbnails[0].item_id, "photo");
    assert_eq!(
        graph.download_thumbnail(&thumbnails[0]).await.unwrap(),
        [255, 216, 255, 217]
    );
}

#[tokio::test]
async fn ranged_cdn_reads_refresh_expired_urls_and_validate_responses() {
    for expired_status in [401, 403, 410] {
        let server = MockServer::start().await;
        let cdn = MockServer::start().await;
        let graph = mock_graph(&server);
        let metadata = || {
            Mock::given(path("/v1.0/me/drive/items/file"))
                .and(header("authorization", "Bearer synthetic-access"))
        };
        let expired = metadata()
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "@microsoft.graph.downloadUrl":format!("{}/expired", cdn.uri())
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount_as_scoped(&server)
            .await;
        metadata()
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "@microsoft.graph.downloadUrl":format!("{}/content", cdn.uri())
            })))
            .expect(7)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/expired"))
            .and(header("range", "bytes=2-6"))
            .and(anonymous)
            .respond_with(ResponseTemplate::new(expired_status))
            .expect(1)
            .mount(&cdn)
            .await;

        for (range, status, content_range, body) in [
            ("bytes=2-6", 206, Some("bytes 2-4/5"), "cde"),
            ("bytes=9-10", 416, Some("bytes */5"), ""),
            ("bytes=0-1", 200, None, "abcde"),
            ("bytes=1-2", 200, None, "abcde"),
            ("bytes=0-2", 206, Some("bytes 1-3/5"), "bcd"),
            ("bytes=0-3", 206, Some("bytes 0-3/5"), "abc"),
        ] {
            let mut response = ResponseTemplate::new(status).set_body_string(body);
            if let Some(value) = content_range {
                response = response.insert_header("content-range", value);
            }
            Mock::given(method("GET"))
                .and(path("/content"))
                .and(anonymous)
                .and(header("range", range))
                .respond_with(response)
                .expect(1)
                .mount(&cdn)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/content"))
            .and(anonymous)
            .and(header("range", "bytes=0-0"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-encoding", "gzip")
                    .set_body_bytes(vec![0x1f, 0x8b]),
            )
            .expect(1)
            .mount(&cdn)
            .await;
        assert_eq!(graph.read_range("file", 2, 5).await.unwrap(), b"cde");
        assert_eq!(
            graph.read_range("file", 0, 1).await,
            Err(CloudError::InvalidResponse)
        );
        assert_eq!(graph.read_range("file", 9, 2).await.unwrap(), b"");
        assert_eq!(graph.read_range("file", 0, 2).await.unwrap(), b"ab");
        for (offset, length) in [(1, 2), (0, 3), (0, 4)] {
            assert_eq!(
                graph.read_range("file", offset, length).await,
                Err(CloudError::InvalidResponse)
            );
        }
        assert_eq!(graph.read_range("unused", u64::MAX, 0).await.unwrap(), b"");
        assert_eq!(
            graph.read_range("unused", u64::MAX, 2).await,
            Err(CloudError::InvalidResponse)
        );
        drop(expired);
    }
}

#[tokio::test]
async fn throttling_pauses_concurrent_metadata_mutations_and_cdn_requests() {
    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    let graph = mock_graph(&server);
    let thumbnail = RemoteThumbnail {
        item_id: "file".into(),
        url: format!("{}/thumbnail", cdn.uri()),
    };
    for (trigger, status) in [("first", 429), ("second", 503)] {
        Mock::given(path(format!("/{trigger}")))
            .respond_with(ResponseTemplate::new(status).insert_header("Retry-After", "1"))
            .expect(1)
            .mount(&server)
            .await;
        let link = format!("{}/{trigger}", server.uri());
        assert!(matches!(graph.delta(DeltaRequest::Link(link)).await,
            Err(CloudError::Throttled { retry_after }) if retry_after == Duration::from_secs(1)));
        let stopped = Instant::now();
        let operations = async {
            tokio::join!(
                graph.delete_item("file"),
                graph.list_thumbnails("folder"),
                graph.download_thumbnail(&thumbnail),
            )
        };
        let timed = move |_: &wiremock::Request| {
            stopped.elapsed() + Duration::from_millis(20) >= Duration::from_secs(1)
        };
        let delete = Mock::given(method("DELETE"))
            .and(timed)
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount_as_scoped(&server)
            .await;
        let list = Mock::given(path("/v1.0/me/drive/items/folder/children"))
            .and(timed)
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value":[]})))
            .expect(1)
            .mount_as_scoped(&server)
            .await;
        let payload = Mock::given(path("/thumbnail"))
            .and(timed)
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![42]))
            .expect(1)
            .mount_as_scoped(&cdn)
            .await;
        let (deleted, listed, downloaded) = operations.await;
        deleted.unwrap();
        assert!(listed.unwrap().is_empty());
        assert_eq!(downloaded.unwrap(), [42]);
        drop((delete, list, payload));
    }
}

#[tokio::test]
async fn simple_upload_folder_move_and_recycle_bin_use_personal_drive() {
    use wiremock::matchers::{body_bytes, body_json};
    let server = MockServer::start().await;
    let graph = mock_graph(&server);
    let item = json!({"id":"item","name":"photo #.jpg","parentReference":{"id":"folder"}});
    let bytes = vec![7; 10 * 1024 * 1024];
    Mock::given(method("PUT"))
        .and(path(
            "/v1.0/me/drive/items/folder:/photo%20%23.jpg:/content",
        ))
        .and(query_param("@microsoft.graph.conflictBehavior", "fail"))
        .and(header("authorization", "Bearer synthetic-access"))
        .and(body_bytes(bytes.clone()))
        .respond_with(ResponseTemplate::new(201).set_body_json(&item))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/v1.0/me/drive/items/item/content"))
        .and(header("if-match", "\"old\""))
        .and(body_bytes(b"update".to_vec()))
        .respond_with(ResponseTemplate::new(412))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1.0/me/drive/items/root/children"))
        .and(body_json(
            json!({"name":"photos","folder":{},"@microsoft.graph.conflictBehavior":"fail"}),
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id":"folder","name":"photos","folder":{},"parentReference":{"id":"root"}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/v1.0/me/drive/items/item"))
        .and(body_json(
            json!({"name":"photo #.jpg","parentReference":{"id":"folder"}}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(&item))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v1.0/me/drive/items/item"))
        .and(header("authorization", "Bearer synthetic-access"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(
        graph.create_folder("root", "photos").await.unwrap().kind,
        ItemKind::Folder
    );
    assert_eq!(
        graph
            .upload(
                UploadTarget::New {
                    parent_id: "folder",
                    name: "photo #.jpg"
                },
                &bytes,
                None
            )
            .await
            .unwrap()
            .item_id,
        "item"
    );
    assert_eq!(
        graph
            .upload(
                UploadTarget::Existing { item_id: "item" },
                b"update",
                Some("\"old\"")
            )
            .await,
        Err(CloudError::Conflict)
    );
    assert_eq!(
        graph
            .move_item("item", "folder", "photo #.jpg")
            .await
            .unwrap()
            .parent_id
            .as_deref(),
        Some("folder")
    );
    graph.delete_item("item").await.unwrap();
}

#[tokio::test]
async fn upload_session_resumes_from_server_offset_and_maps_conflict() {
    use wiremock::matchers::body_bytes;
    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    let graph = mock_graph(&server);
    let session = || {
        Mock::given(method("POST"))
            .and(path("/v1.0/me/drive/items/file/createUploadSession"))
            .and(header("authorization", "Bearer synthetic-access"))
    };
    session()
        .and(header("if-match", "old"))
        .respond_with(ResponseTemplate::new(412))
        .expect(1)
        .mount(&server)
        .await;
    session()
        .and(header("if-match", "fresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "uploadUrl":format!("{}/upload?sig=temporary",cdn.uri())
        })))
        .expect(1)
        .mount(&server)
        .await;
    let chunk = || {
        Mock::given(method("PUT"))
            .and(path("/upload"))
            .and(query_param("sig", "temporary"))
            .and(anonymous)
    };
    chunk()
        .and(header("content-range", "bytes 0-5242879/10485763"))
        .and(header("content-length", "5242880"))
        .and(body_bytes(vec![1; 5 * 1024 * 1024]))
        .respond_with(
            ResponseTemplate::new(202).set_body_json(json!({"nextExpectedRanges":["5242880-"]})),
        )
        .expect(1)
        .mount(&cdn)
        .await;
    chunk()
        .and(header("content-range", "bytes 5242880-10485759/10485763"))
        .and(header("content-length", "5242880"))
        .and(body_bytes(vec![2; 5 * 1024 * 1024]))
        .respond_with(ResponseTemplate::new(416))
        .expect(1)
        .mount(&cdn)
        .await;
    Mock::given(method("GET"))
        .and(path("/upload"))
        .and(anonymous)
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"nextExpectedRanges":["10485760-"]})),
        )
        .expect(1)
        .mount(&cdn)
        .await;
    chunk()
        .and(header("content-range", "bytes 10485760-10485762/10485763"))
        .and(header("content-length", "3"))
        .and(body_bytes(vec![3; 3]))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id":"file","eTag":"updated"})),
        )
        .expect(1)
        .mount(&cdn)
        .await;

    let mut bytes = vec![1; 5 * 1024 * 1024];
    bytes.extend(vec![2; 5 * 1024 * 1024]);
    bytes.extend([3; 3]);
    let target = UploadTarget::Existing { item_id: "file" };
    assert_eq!(
        graph.upload(target, &bytes, Some("old")).await,
        Err(CloudError::Conflict)
    );
    assert_eq!(
        graph
            .upload(target, &bytes, Some("fresh"))
            .await
            .unwrap()
            .etag
            .as_deref(),
        Some("updated")
    );
}

#[tokio::test]
async fn thumbnail_download_negotiates_tls_http2_without_bearer() {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tokio_rustls::rustls::{ServerConfig, pki_types::PrivatePkcs8KeyDer};

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = cert.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key.into())
        .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let stream = tokio_rustls::TlsAcceptor::from(Arc::new(config))
            .accept(stream)
            .await
            .unwrap();
        assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(
                    |request: hyper::Request<hyper::body::Incoming>| async move {
                        assert_eq!(request.version(), hyper::Version::HTTP_2);
                        assert!(!request.headers().contains_key(hyper::header::AUTHORIZATION));
                        Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(
                            Bytes::from_static(b"image"),
                        )))
                    },
                ),
            )
            .await
            .unwrap();
    });
    let mock = MockServer::start().await;
    let mut graph = mock_graph(&mock);
    graph.client = crate::http_client_builder()
        .add_root_certificate(reqwest::Certificate::from_der(certificate.as_ref()).unwrap())
        .build()
        .unwrap();
    let thumbnail = RemoteThumbnail {
        item_id: "photo".into(),
        url: format!("https://localhost:{port}/image"),
    };
    assert_eq!(
        graph.download_thumbnail(&thumbnail).await.unwrap(),
        b"image"
    );
    drop(graph);
    server.await.unwrap();
}
