use arrow::datatypes::Schema;

trait DataSource {
  fn schema() -> Schema;
}