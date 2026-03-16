#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFocusNav {
    Consumed,
    ExitFocus,
    EnterValueFocus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NavIntent {
    LineUp,
    LineDown,
    PageUp,
    PageDown,
    Home,
    End,
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JsonFocusState {
    pub key_index: usize,
    pub value_focus: bool,
}

impl JsonFocusState {
    pub fn handle_nav(&mut self, intent: NavIntent, key_count: usize) -> JsonFocusNav {
        match intent {
            NavIntent::LineUp => {
                self.key_index = self.key_index.saturating_sub(1);
                self.value_focus = false;
                JsonFocusNav::Consumed
            }
            NavIntent::LineDown => {
                if self.key_index + 1 < key_count {
                    self.key_index += 1;
                }
                self.value_focus = false;
                JsonFocusNav::Consumed
            }
            NavIntent::PageUp => {
                self.key_index = self.key_index.saturating_sub(30);
                self.value_focus = false;
                JsonFocusNav::Consumed
            }
            NavIntent::PageDown => {
                self.key_index = (self.key_index + 30).min(key_count.saturating_sub(1));
                self.value_focus = false;
                JsonFocusNav::Consumed
            }
            NavIntent::Home => {
                self.key_index = 0;
                self.value_focus = false;
                JsonFocusNav::Consumed
            }
            NavIntent::End => {
                self.key_index = key_count.saturating_sub(1);
                self.value_focus = false;
                JsonFocusNav::Consumed
            }
            NavIntent::Left => {
                if self.value_focus {
                    self.value_focus = false;
                    JsonFocusNav::Consumed
                } else {
                    JsonFocusNav::ExitFocus
                }
            }
            NavIntent::Right => JsonFocusNav::EnterValueFocus,
        }
    }

    pub fn enter(&mut self) {
        self.key_index = 0;
        self.value_focus = false;
    }

    pub fn exit(&mut self) {
        self.value_focus = false;
    }

    pub fn clamp(&mut self, key_count: usize) {
        if key_count == 0 {
            self.key_index = 0;
            self.value_focus = false;
            return;
        }
        self.key_index = self.key_index.min(key_count.saturating_sub(1));
    }
}
