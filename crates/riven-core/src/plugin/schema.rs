use std::borrow::Cow;

use serde::Serialize;

/// Input-type hint for a [`SettingField`]. Serialises to the wire string the
/// frontend's renderer matches on, so an invalid type is a compile error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Password,
    Url,
    Number,
    Boolean,
    Textarea,
    Select,
    Object,
    Dictionary,
    StringArray,
    NullableBoolean,
    CustomRank,
}

/// Layout hint for container (`Object`) fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Display {
    Grid,
    Tabs,
}

/// Describes one configurable setting field for a plugin.
/// Used to render the settings UI dynamically on the frontend.
///
/// String fields are `Cow<'static, str>` so a schema can be built either from
/// static literals (zero-alloc, the common case) or generated at runtime from
/// data — e.g. the rank-settings schema generated from the ranking model.
#[derive(Debug, Clone, Serialize)]
pub struct SettingField {
    pub key: Cow<'static, str>,
    pub label: Cow<'static, str>,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<Cow<'static, str>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<SettingField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_fields: Option<Vec<SettingField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_placeholder: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add_label: Option<Cow<'static, str>>,
    /// Optional grouping label. Fields sharing the same `section` are
    /// rendered together under one heading; fields without a section are
    /// rendered in the default unnamed group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub section: Option<Cow<'static, str>>,
    /// Optional layout hint for container fields (`object`):
    /// `Grid` lays the children out in a responsive grid; `Tabs` renders each
    /// child (itself an object) as a tab panel. Absent = stacked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<Display>,
    /// For `nullable_boolean` fields: the labels for the true / false choices
    /// (the unset choice is rendered generically). Lets the frontend avoid
    /// hardcoding per-field wording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub true_label: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub false_label: Option<Cow<'static, str>>,
}

impl SettingField {
    pub fn new(
        key: impl Into<Cow<'static, str>>,
        label: impl Into<Cow<'static, str>>,
        field_type: FieldType,
    ) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
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
            section: None,
            display: None,
            true_label: None,
            false_label: None,
        }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn with_default(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.default_value = Some(v.into());
        self
    }

    pub fn with_placeholder(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.placeholder = Some(v.into());
        self
    }

    pub fn with_description(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(v.into());
        self
    }

    pub fn with_options(mut self, values: &[&'static str]) -> Self {
        self.options = Some(values.iter().map(|s| Cow::Borrowed(*s)).collect());
        self
    }

    /// Set options from owned/generated strings (e.g. a runtime-derived list).
    pub fn with_options_owned(
        mut self,
        values: impl IntoIterator<Item = impl Into<Cow<'static, str>>>,
    ) -> Self {
        self.options = Some(values.into_iter().map(Into::into).collect());
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

    pub fn with_key_placeholder(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.key_placeholder = Some(v.into());
        self
    }

    pub fn with_add_label(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.add_label = Some(v.into());
        self
    }

    pub fn with_section(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.section = Some(v.into());
        self
    }

    pub fn with_display(mut self, v: Display) -> Self {
        self.display = Some(v);
        self
    }

    pub fn with_bool_labels(
        mut self,
        true_label: impl Into<Cow<'static, str>>,
        false_label: impl Into<Cow<'static, str>>,
    ) -> Self {
        self.true_label = Some(true_label.into());
        self.false_label = Some(false_label.into());
        self
    }
}
