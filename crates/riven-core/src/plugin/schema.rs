use serde::Serialize;

/// Describes one configurable setting field for a plugin.
/// Used to render the settings UI dynamically on the frontend.
#[derive(Debug, Clone, Serialize)]
pub struct SettingField {
    pub key: &'static str,
    pub label: &'static str,
    /// Input type hint: "text" | "password" | "url" | "number" | "boolean" | "textarea"
    #[serde(rename = "type")]
    pub field_type: &'static str,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<&'static str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<SettingField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_fields: Option<Vec<SettingField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_placeholder: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add_label: Option<&'static str>,
}

impl SettingField {
    pub const fn new(key: &'static str, label: &'static str, field_type: &'static str) -> Self {
        Self {
            key,
            label,
            field_type,
            required: false,
            default_value: None,
            placeholder: None,
            description: None,
            options: None,
            fields: None,
            item_fields: None,
            key_placeholder: None,
            add_label: None,
        }
    }

    pub const fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub const fn with_default(mut self, v: &'static str) -> Self {
        self.default_value = Some(v);
        self
    }

    pub const fn with_placeholder(mut self, v: &'static str) -> Self {
        self.placeholder = Some(v);
        self
    }

    pub const fn with_description(mut self, v: &'static str) -> Self {
        self.description = Some(v);
        self
    }

    pub fn with_options(mut self, values: &[&'static str]) -> Self {
        self.options = Some(values.to_vec());
        self
    }

    pub fn with_fields(mut self, fields: Vec<SettingField>) -> Self {
        self.fields = Some(fields);
        self
    }

    pub fn with_item_fields(mut self, fields: Vec<SettingField>) -> Self {
        self.item_fields = Some(fields);
        self
    }

    pub const fn with_key_placeholder(mut self, v: &'static str) -> Self {
        self.key_placeholder = Some(v);
        self
    }

    pub const fn with_add_label(mut self, v: &'static str) -> Self {
        self.add_label = Some(v);
        self
    }
}
