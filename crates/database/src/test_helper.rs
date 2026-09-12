use anyhow::Result;
use tokio_postgres::NoTls;

pub(crate) async fn test_query_helper(sql_query: &str) -> Result<()> {
    let host = "localhost".to_string();
    let port = "26257".to_string();
    let user = "root".to_string();
    let dbname = "accounting_app".to_string();
    let url = format!("postgresql://{}@{}:{}/{}", user, host, port, dbname);

    let (client, connection) =
        tokio_postgres::connect(&url, NoTls).await.expect("Failed to connect to CockroachDB");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("CockroachDB connection error: {}", e);
        }
    });

    client.prepare(sql_query).await?;
    Ok(())
}
