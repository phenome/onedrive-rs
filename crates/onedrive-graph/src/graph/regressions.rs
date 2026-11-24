use super::*;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::tests::{anonymous, mock_graph};

#[tokio::test]
async fn disappeared_upload_session_checks_destination_bytes() {
    use std::io::Write;
    let bytes = vec![42; SIMPLE_UPLOAD_LIMIT + 1];
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&bytes).unwrap();
    let source = UploadSource::File {
        file: &file,
        size: bytes.len(),
    };
    for (new, matches, final_status, expected) in [
        (false, true, 201, Ok("stored")),
        (false, true, 502, Ok("stored")),
        (true, true, 502, Ok("stored")),
        (false, false, 502, Err(CloudError::Conflict)),
        (true, false, 502, Err(CloudError::Conflict)),
        (false, true, 200, Ok("stored")),
        (true, true, 404, Ok("stored")),
        (true, true, 409, Err(CloudError::Conflict)),
    ] {
        let graph = MockServer::start().await;
        let cdn = MockServer::start().await;
        let (target, destination, session, etag) = if new {
            (
                UploadTarget::New {
                    parent_id: "parent",
                    name: "a b",
                },
                "/v1.0/me/drive/items/parent:/a%20b",
                "/v1.0/me/drive/items/parent:/a%20b:/createUploadSession",
                None,
            )
        } else {
            (
                UploadTarget::Existing { item_id: "stored" },
                "/v1.0/me/drive/items/stored",
                "/v1.0/me/drive/items/stored/createUploadSession",
                Some("before"),
            )
        };
        let create = Mock::given(method("POST")).and(path(session));
        let create = if let Some(etag) = etag {
            create.and(header("if-match", etag))
        } else {
            create
        };
        create
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "uploadUrl": format!("{}/session", cdn.uri())
            })))
            .expect(1)
            .mount(&graph)
            .await;
        for (range, next) in [
            ("bytes 0-5242879/10485761", "5242880-"),
            ("bytes 5242880-10485759/10485761", "10485760-"),
        ] {
            Mock::given(method("PUT"))
                .and(path("/session"))
                .and(anonymous)
                .and(header("content-range", range))
                .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                    "nextExpectedRanges": [next]
                })))
                .expect(1)
                .mount(&cdn)
                .await;
        }
        let final_reply = if final_status == 201 {
            ResponseTemplate::new(201)
                .set_body_json(json!({"id": "stored", "eTag": "after"}))
                .set_delay(Duration::from_secs(3))
        } else {
            ResponseTemplate::new(final_status)
        };
        Mock::given(method("PUT"))
            .and(path("/session"))
            .and(anonymous)
            .and(header("content-range", "bytes 10485760-10485760/10485761"))
            .respond_with(final_reply)
            .expect(1)
            .mount(&cdn)
            .await;
        Mock::given(method("GET"))
            .and(path("/session"))
            .and(anonymous)
            .respond_with(ResponseTemplate::new(404))
            .expect(u64::from(matches!(final_status, 201 | 502)))
            .mount(&cdn)
            .await;

        let probes = if final_status == 409 { 0 } else { 2 };
        Mock::given(method("GET"))
            .and(path(destination))
            .and(header("authorization", "Bearer synthetic-access"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "stored", "parentReference": {"id": "parent"}, "name": "a b",
                "size": bytes.len(), "eTag": "after", "cTag": "uploaded-content", "file": {},
                "@microsoft.graph.downloadUrl": format!("{}/committed", cdn.uri())
            })))
            .expect(probes)
            .mount(&graph)
            .await;
        let mut content = bytes.clone();
        if !matches {
            *content.last_mut().unwrap() = 0;
        }
        Mock::given(method("GET"))
            .and(path("/committed"))
            .and(anonymous)
            .respond_with(ResponseTemplate::new(200).set_body_bytes(content))
            .expect(probes / 2)
            .mount(&cdn)
            .await;
        let mut graph_drive = mock_graph(&graph);
        if final_status == 201 {
            graph_drive.client = crate::http_client_builder()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap();
        }
        let result = graph_drive.upload_from(target, source, etag).await;
        assert_eq!(
            result
                .as_ref()
                .map(|item| item.item_id.as_str())
                .map_err(Clone::clone),
            expected
        );
    }
}

#[tokio::test]
async fn signed_urls_are_fresh_across_delta_and_full_scan() {
    for (request, changes, missing) in [
        (
            DeltaRequest::Link(String::new()),
            json!([{"id":"file","eTag":"updated"}]),
            false,
        ),
        (
            DeltaRequest::Link(String::new()),
            json!([{"id":"file","deleted":{}}]),
            true,
        ),
        (DeltaRequest::Initial, json!([]), true),
    ] {
        let server = MockServer::start().await;
        let cdn = MockServer::start().await;
        let graph = mock_graph(&server);
        let old = read_metadata(&server, &cdn, "/old", Duration::ZERO).await;
        mount_read(&cdn, "/old", "o").await;
        assert_eq!(graph.read_range("file", 0, 1).await.unwrap(), b"o");
        drop(old);
        let request = match request {
            DeltaRequest::Initial => DeltaRequest::Initial,
            DeltaRequest::Link(_) => DeltaRequest::Link(format!("{}/delta", server.uri())),
        };
        Mock::given(method("GET"))
            .and(|request: &wiremock::Request| request.url.path().ends_with("/delta"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "value": changes, "@odata.deltaLink": format!("{}/delta", server.uri())
            })))
            .expect(1)
            .mount(&server)
            .await;
        graph.delta(request).await.unwrap();

        if missing {
            Mock::given(method("GET"))
                .and(path("/v1.0/me/drive/items/file"))
                .respond_with(ResponseTemplate::new(404))
                .expect(1)
                .mount(&server)
                .await;
            assert_eq!(
                graph.read_range("file", 0, 1).await,
                Err(CloudError::NotFound)
            );
        } else {
            let _fresh = read_metadata(&server, &cdn, "/fresh", Duration::ZERO).await;
            mount_read(&cdn, "/fresh", "n").await;
            assert_eq!(graph.read_range("file", 0, 1).await.unwrap(), b"n");
        }
    }
}

#[tokio::test]
async fn delayed_metadata_lookup_does_not_poison_subsequent_reads() {
    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    let graph = Arc::new(mock_graph(&server));
    let old = read_metadata(&server, &cdn, "/old", Duration::from_millis(250)).await;
    mount_read(&cdn, "/old", "o").await;
    mount_read(&cdn, "/fresh", "n").await;
    let in_flight = {
        let graph = graph.clone();
        tokio::spawn(async move { graph.read_range("file", 0, 1).await.unwrap() })
    };
    old.wait_until_satisfied().await;
    let fresh = read_metadata(&server, &cdn, "/fresh", Duration::ZERO).await;
    assert_eq!(graph.read_range("file", 0, 1).await.unwrap(), b"n");
    assert_eq!(in_flight.await.unwrap(), b"o");
    drop(fresh);
    let _fresh = read_metadata(&server, &cdn, "/fresh", Duration::ZERO).await;
    assert_eq!(graph.read_range("file", 0, 1).await.unwrap(), b"n");
}

async fn read_metadata(
    server: &MockServer,
    cdn: &MockServer,
    route: &str,
    delay: Duration,
) -> wiremock::MockGuard {
    Mock::given(method("GET"))
        .and(path("/v1.0/me/drive/items/file"))
        .and(header("authorization", "Bearer synthetic-access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(delay)
                .set_body_json(json!({
                    "@microsoft.graph.downloadUrl": format!("{}{route}", cdn.uri())
                })),
        )
        .up_to_n_times(1)
        .expect(1)
        .mount_as_scoped(server)
        .await
}

async fn mount_read(cdn: &MockServer, route: &str, byte: &str) {
    Mock::given(method("GET"))
        .and(path(route))
        .and(anonymous)
        .and(header("range", "bytes=0-0"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-0/1")
                .set_body_string(byte),
        )
        .mount(cdn)
        .await;
}

#[tokio::test]
async fn ambiguous_simple_upload_requires_current_destination_and_complete_bytes() {
    for (first, second, content, expected) in [
        (
            Some(("file", "after", Some("same-content"))),
            Some(("file", "after", Some("same-content"))),
            "sent",
            Ok("after"),
        ),
        (
            Some(("file", "after", Some("uploaded-content"))),
            Some(("file", "after", Some("peer-content"))),
            "sent",
            Err(CloudError::Unavailable),
        ),
        (
            Some(("file", "after", None)),
            None,
            "",
            Err(CloudError::Unavailable),
        ),
        (
            Some(("file", "after", Some(""))),
            None,
            "",
            Err(CloudError::Unavailable),
        ),
        (
            Some(("file", "after", Some("uploaded-content"))),
            Some(("file", "after", None)),
            "sent",
            Err(CloudError::Unavailable),
        ),
        (None, None, "", Err(CloudError::Unavailable)),
        (
            Some(("other", "after", Some("same-content"))),
            None,
            "",
            Err(CloudError::Unavailable),
        ),
        (
            Some(("file", "before", Some("same-content"))),
            None,
            "",
            Err(CloudError::Unavailable),
        ),
        (
            Some(("file", "after", Some("same-content"))),
            Some(("file", "later", Some("same-content"))),
            "sent",
            Err(CloudError::Unavailable),
        ),
        (
            Some(("file", "after", Some("same-content"))),
            None,
            "sent!",
            Err(CloudError::Unavailable),
        ),
        (
            Some(("file", "after", Some("same-content"))),
            None,
            "sen",
            Err(CloudError::Unavailable),
        ),
    ] {
        let server = MockServer::start().await;
        let cdn = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1.0/me/drive/items/file/content"))
            .and(header("if-match", "before"))
            .and(wiremock::matchers::body_bytes(b"sent"))
            .respond_with(ResponseTemplate::new(502))
            .expect(1)
            .mount(&server)
            .await;
        let mut mocks = Vec::new();
        let variants: Vec<_> = if second.is_some() {
            vec![first, second]
        } else {
            vec![first]
        };
        for version in variants {
            let reply = if let Some((id, etag, ctag)) = version {
                let mut metadata = json!({
                    "id": id, "size": 4, "eTag": etag, "file": {},
                    "@microsoft.graph.downloadUrl": format!("{}/committed", cdn.uri())
                });
                if let Some(tag) = ctag {
                    metadata["cTag"] = json!(tag);
                }
                ResponseTemplate::new(200).set_body_json(metadata)
            } else {
                ResponseTemplate::new(404)
            };
            mocks.push(
                Mock::given(method("GET"))
                    .and(path("/v1.0/me/drive/items/file"))
                    .respond_with(reply)
                    .up_to_n_times(1)
                    .expect(1)
                    .mount_as_scoped(&server)
                    .await,
            );
        }
        Mock::given(method("GET"))
            .and(path("/committed"))
            .and(anonymous)
            .respond_with(ResponseTemplate::new(200).set_body_string(content))
            .expect(u64::from(!content.is_empty()))
            .mount(&cdn)
            .await;
        let target = UploadTarget::Existing { item_id: "file" };
        let uploaded = mock_graph(&server)
            .upload(target, b"sent", Some("before"))
            .await;
        assert_eq!(
            uploaded
                .as_ref()
                .map(|item| item.etag.as_deref().unwrap())
                .map_err(Clone::clone),
            expected,
        );
    }
}
