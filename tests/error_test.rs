use std::collections::HashMap;
use veloxx::dataframe::DataFrame;
use veloxx::error::VeloxxError;
use veloxx::series::Series;

#[test]
fn test_mismatched_lengths() {
    let mut columns = HashMap::new();
    columns.insert(
        "a".to_string(),
        Series::new_i32("a", vec![Some(1), Some(2)]),
    );
    columns.insert("b".to_string(), Series::new_i32("b", vec![Some(1)]));

    let _err = DataFrame::new(columns).unwrap_err();
    // assert_eq!(err, VeloxxError::MismatchedLengths { expected: 2, found: 1 });
}

#[test]
fn test_column_not_found() {
    let df = DataFrame::new(HashMap::new()).unwrap();
    let err = df.select_columns(vec!["a".to_string()]).unwrap_err();
    assert_eq!(err, VeloxxError::ColumnNotFound("a".to_string()));
}
