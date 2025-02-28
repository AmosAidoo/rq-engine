use std::{any::Any, sync::Arc};
use arrow::datatypes::DataType;

use crate::error::RqEngineError;

pub trait ColumnVector {
  fn get_type(&self) -> &DataType;
  fn get_value(&self, i: usize) -> Result<Option<Arc<dyn Any>>, RqEngineError>;
  fn size(&self) -> usize;
}