use datafusion::prelude::*;
use datafusion_physical_expr::regexp_extract::create_regexp_extract_udf;

#[tokio::test]
async fn test_regexp_extract_integration() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udf(create_regexp_extract_udf());

    // Create in-memory table
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "abc123", "xyz789", "no_digits"
        ]))],
    )?;

    ctx.register_batch("my_table", batch)?;

    let df = ctx.table("my_table").await?;
    let df = df.select(vec![
        col("text"),
        udf("regexp_extract")(vec![
            col("text"),
            lit(r"(\d+)"),
            lit(0)
        ]).alias("extracted"),
    ])?;

    let results = df.collect().await?;

    let expected = vec![
        vec!["abc123", "123"],
        vec!["xyz789", "789"],
        vec!["no_digits", ""],
    ];

    for (row, expect) in results[0].columns()[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .zip(expected.iter())
    {
        println!("{:?}", row);
    }

    Ok(())
}
