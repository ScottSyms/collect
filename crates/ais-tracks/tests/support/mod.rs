//! A test Iceberg catalog with real metadata and real files on the local
//! filesystem, plus the tiny slice of the REST API that `ais-compact`'s
//! hand-built `replace` commit uses. Together they let the whole daily flow run
//! for real without an external service.
#![allow(dead_code)]

pub mod scenario;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent, TableRequirement, TableUpdate,
};

#[derive(Debug, Default)]
struct Inner {
    namespaces: HashSet<String>,
    tables: HashMap<TableIdent, Table>,
    versions: HashMap<TableIdent, u32>,
}

#[derive(Debug, Clone)]
pub struct MockCatalog {
    inner: Arc<Mutex<Inner>>,
    root: PathBuf,
}

fn err(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::Unexpected, msg.into())
}

fn location(root: &Path, ident: &TableIdent, version: u32) -> (String, String) {
    let table = format!("file://{}/{}/{}", root.display(), ident.namespace().to_url_string(), ident.name());
    let meta = format!("{table}/metadata/{version:05}-{}.metadata.json", uuid::Uuid::new_v4());
    (table, meta)
}

impl MockCatalog {
    pub fn new(root: &Path) -> Self {
        Self {
            inner: Arc::default(),
            root: root.to_path_buf(),
        }
    }

    /// Serves the REST commit endpoint over the same tables; returns its base URL.
    pub async fn serve(&self) -> String {
        async fn config() -> Json<serde_json::Value> {
            Json(serde_json::json!({ "defaults": {}, "overrides": {} }))
        }
        async fn commit(
            State(cat): State<MockCatalog>,
            UrlPath((ns, name)): UrlPath<(String, String)>,
            Json(body): Json<serde_json::Value>,
        ) -> (StatusCode, String) {
            let ident = TableIdent::new(NamespaceIdent::from_strs(ns.split('\u{1f}')).unwrap(), name);
            let reqs: Vec<TableRequirement> =
                serde_json::from_value(body["requirements"].clone()).unwrap();
            let updates: Vec<TableUpdate> = serde_json::from_value(body["updates"].clone()).unwrap();
            let mut inner = cat.inner.lock().unwrap();
            let Some(table) = inner.tables.get(&ident).cloned() else {
                return (StatusCode::NOT_FOUND, "no such table".into());
            };
            for r in &reqs {
                if r.check(Some(table.metadata())).is_err() {
                    return (StatusCode::CONFLICT, "requirement failed".into());
                }
            }
            let mut b = table
                .metadata()
                .clone()
                .into_builder(table.metadata_location().map(String::from));
            for u in updates {
                b = match u.apply(b) {
                    Ok(b) => b,
                    Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()),
                };
            }
            let metadata = match b.build() {
                Ok(m) => m.metadata,
                Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()),
            };
            let v = inner.versions.entry(ident.clone()).or_insert(0);
            *v += 1;
            let (_, meta_loc) = location(&cat.root, &ident, *v);
            let new = Table::builder()
                .metadata(metadata)
                .identifier(ident.clone())
                .file_io(FileIO::new_with_fs())
                .metadata_location(meta_loc)
                .build()
                .unwrap();
            inner.tables.insert(ident, new);
            (StatusCode::OK, "{}".into())
        }
        let app = Router::new()
            .route("/v1/config", get(config))
            .route("/v1/namespaces/:ns/tables/:name", post(commit))
            .with_state(self.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }
}

#[async_trait]
impl Catalog for MockCatalog {
    async fn list_namespaces(&self, _: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .namespaces
            .iter()
            .map(|n| NamespaceIdent::new(n.clone()))
            .collect())
    }

    async fn create_namespace(&self, ns: &NamespaceIdent, props: HashMap<String, String>) -> Result<Namespace> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.namespaces.insert(ns.to_url_string()) {
            return Err(err("namespace already exists"));
        }
        Ok(Namespace::with_properties(ns.clone(), props))
    }

    async fn get_namespace(&self, ns: &NamespaceIdent) -> Result<Namespace> {
        Ok(Namespace::new(ns.clone()))
    }

    async fn namespace_exists(&self, ns: &NamespaceIdent) -> Result<bool> {
        Ok(self.inner.lock().unwrap().namespaces.contains(&ns.to_url_string()))
    }

    async fn update_namespace(&self, _: &NamespaceIdent, _: HashMap<String, String>) -> Result<()> {
        Err(err("unsupported"))
    }

    async fn drop_namespace(&self, _: &NamespaceIdent) -> Result<()> {
        Err(err("unsupported"))
    }

    async fn list_tables(&self, ns: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .tables
            .keys()
            .filter(|t| t.namespace() == ns)
            .cloned()
            .collect())
    }

    async fn create_table(&self, ns: &NamespaceIdent, mut creation: TableCreation) -> Result<Table> {
        let ident = TableIdent::new(ns.clone(), creation.name.clone());
        let mut inner = self.inner.lock().unwrap();
        if inner.tables.contains_key(&ident) {
            return Err(err("table already exists"));
        }
        let (table_loc, meta_loc) = location(&self.root, &ident, 0);
        creation.location.get_or_insert(table_loc);
        let metadata = iceberg::spec::TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        let table = Table::builder()
            .metadata(metadata)
            .identifier(ident.clone())
            .file_io(FileIO::new_with_fs())
            .metadata_location(meta_loc)
            .build()?;
        inner.tables.insert(ident, table.clone());
        Ok(table)
    }

    async fn load_table(&self, t: &TableIdent) -> Result<Table> {
        self.inner
            .lock()
            .unwrap()
            .tables
            .get(t)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::TableNotFound, format!("no table {t}")))
    }

    async fn drop_table(&self, _: &TableIdent) -> Result<()> {
        Err(err("unsupported"))
    }

    async fn table_exists(&self, t: &TableIdent) -> Result<bool> {
        Ok(self.inner.lock().unwrap().tables.contains_key(t))
    }

    async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> Result<()> {
        Err(err("unsupported"))
    }

    async fn register_table(&self, _: &TableIdent, _: String) -> Result<Table> {
        Err(err("unsupported"))
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let ident = commit.identifier().clone();
        let mut inner = self.inner.lock().unwrap();
        let table = inner
            .tables
            .get(&ident)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::TableNotFound, format!("no table {ident}")))?;
        let new = commit.apply(table)?;
        inner.tables.insert(ident, new.clone());
        Ok(new)
    }
}
