# Build with makepkg from a checkout containing the onedrive-rs binary crate.
pkgname=onedrive-rs
pkgver=0.1.0
pkgrel=1
pkgdesc='Cloud-first OneDrive filesystem for Linux'
arch=('x86_64')
url='https://github.com/phenome/onedrive-rs'
depends=('fuse3' 'glib2>=2.56' 'glibc' 'libgcc' 'python')
makedepends=('cargo' 'pkgconf')
# Cargo links native objects from ring and bundled SQLite with Rust's linker.
options=('!lto')
optdepends=('nautilus-python: status emblems and Pin/Purge actions in Nautilus')

build() {
  cd "$startdir"
  CARGO_TARGET_DIR="$srcdir/target" cargo build --locked --release --bin onedrive-rs
}

package() {
  install -Dm644 "$startdir/desktop/systemd/onedrive-rs.service" \
    "$pkgdir/usr/lib/systemd/user/onedrive-rs.service"
  install -Dm644 "$startdir/desktop/nautilus/onedrive_nautilus.py" \
    "$pkgdir/usr/share/nautilus-python/extensions/onedrive_nautilus.py"
  install -Dm755 "$startdir/desktop/onedrive-rs-bookmark" \
    "$pkgdir/usr/bin/onedrive-rs-bookmark"
  install -Dm755 "$srcdir/target/release/onedrive-rs" "$pkgdir/usr/bin/onedrive-rs"
}
