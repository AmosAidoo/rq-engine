use arrow::datatypes::DataType;
use arrow::datatypes::Field as ArrowField;
use arrow::datatypes::Schema as ArrowSchema;

pub struct Field {
  name: String,
  data_type: DataType
}

impl Field {
  fn to_arrow(&self) -> ArrowField {
    ArrowField::new(self.name.clone(), self.data_type.clone(), false)
  }
}

pub struct Schema {
  fields: Vec<Field>
}

impl Schema {
  fn to_arrow(&self) -> ArrowSchema {
    let arrow_fields: Vec<ArrowField> = self.fields.iter().map(|field| field.to_arrow()).collect();
    ArrowSchema::new(arrow_fields)
  }

  fn project(&self, indices: &[usize]) -> ArrowSchema {
    let new_fields: Vec<ArrowField> = indices.into_iter().map(|i| ArrowField::new(self.fields[*i].name.clone(), self.fields[*i].data_type.clone(), false)).collect();
    ArrowSchema::new(new_fields)
  }
}