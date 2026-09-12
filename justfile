default: fmt stat

fmt:
    @cargo sort -w --grouped
    @cargo sort-derives --order "Debug,...,Deserialize,Serialize"
    @cargo +nightly fmt
    @clear

stat:
    @clear
    git ls-files "crates/accounting_engine/*.rs" | xargs wc -l | tail -1
    git ls-files "crates/*.rs" | xargs wc -l | tail -1
    git ls-files "*.rs" | xargs wc -l | tail -1
    git ls-files | xargs wc -l | tail -1
    git rev-list --count HEAD

dump: fmt stat
    @git ls-files | while read -r f; do file -b --mime-type "$f" | grep -q "^text/" && { echo "=== $f ==="; cat "$f"; } done > codebase.txt

check: fmt
    RUSTFLAGS="-A warnings" cargo check --all-targets
    RUSTFLAGS="-A warnings" cargo check --all-targets --features="server"
    RUSTFLAGS="-A warnings" cargo check --all-targets --features="client"
    RUSTFLAGS="-A warnings" cargo check --all-targets --features="server,database"
    RUSTFLAGS="-A warnings" cargo check --all-targets --features="client,cache"
    RUSTFLAGS="-A warnings" cargo check --all-targets --features="client,ui"
    RUSTFLAGS="-A warnings" cargo check --all-targets --features="client,server,infrastructure"

test: fmt
    RUSTFLAGS="-A warnings" cargo test -- --show-output

warn: fmt
    cargo clippy --all-targets --all-features -- -W clippy::pedantic

test_cover: fmt
    @cargo tarpaulin --out HTML
    @xdg-open /home/hashem/Documents/backup_folder_for_hashem/accounting_app/tarpaulin-report.html

all: fmt check test warn test_cover


check_p crate_name: fmt
    RUSTFLAGS="-A warnings" cargo check --workspace -p {{crate_name}} --all-targets
    RUSTFLAGS="-A warnings" cargo check --workspace -p {{crate_name}} --all-targets --features="server"
    RUSTFLAGS="-A warnings" cargo check --workspace -p {{crate_name}} --all-targets --features="client"
    RUSTFLAGS="-A warnings" cargo check --workspace -p {{crate_name}} --all-targets --features="server,database"
    RUSTFLAGS="-A warnings" cargo check --workspace -p {{crate_name}} --all-targets --features="client,cache"
    RUSTFLAGS="-A warnings" cargo check --workspace -p {{crate_name}} --all-targets --features="client,ui"
    RUSTFLAGS="-A warnings" cargo check --workspace -p {{crate_name}} --all-targets --features="client,server,infrastructure"

test_p crate_name: fmt
    RUSTFLAGS="-A warnings" cargo test -p {{crate_name}} -- --show-output

warn_p crate_name: fmt
    cargo clippy -p {{crate_name}} --all-targets --all-features -- -W clippy::pedantic

test_cover_p crate_name: fmt
    @cargo tarpaulin -p {{crate_name}} --out HTML
    @xdg-open /home/hashem/Documents/backup_folder_for_hashem/accounting_app/tarpaulin-report.html

all_p crate_name:
    @just fmt
    @just check_p {{crate_name}}
    @just test_p {{crate_name}}
    @just warn_p {{crate_name}}
    @just test_cover_p {{crate_name}}


new crate_name:
    cargo new crates/{{crate_name}} --lib --vcs none
