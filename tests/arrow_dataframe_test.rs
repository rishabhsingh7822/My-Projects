//! Tests for Arrow DataFrame implementation

#[cfg(feature = "arrow")]
use veloxx::arrow::{ArrowDataFrame, ArrowSeries};

#[cfg(feature = "arrow")]
#[test]
fn test_arrow_dataframe_creation() {
    let mut df = ArrowDataFrame::new();

    let data1 = vec![Some(1i32), Some(2), Some(3)];
    let series1 = ArrowSeries::new_i32("a", data1);
    df.add_column(series1);

    let data2 = vec![Some(1.0f64), Some(2.0), Some(3.0)];
    let series2 = ArrowSeries::new_f64("b", data2);
    df.add_column(series2);

    assert_eq!(df.column_count(), 2);
    assert_eq!(df.row_count(), 3);
}

#[cfg(feature = "arrow")]
#[test]
fn test_arrow_dataframe_empty() {
    let df = ArrowDataFrame::new();

    assert_eq!(df.column_count(), 0);
    assert_eq!(df.row_count(), 0);
}
