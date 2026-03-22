use std::fmt::{Debug, Display};

pub type Uin = u32;

pub enum TextElement {
    At(u32),
}

impl TextElement {
    pub fn from_at(at: Uin) -> Self {
        TextElement::At(at)
    }

    pub fn at_all() -> Self {
        TextElement::At(0)
    }

    pub fn is_at_all(&self) -> bool {
        matches!(self, TextElement::At(0))
    }

    pub fn at_from_raw_db(payload: &[u8]) -> anyhow::Result<Self> {
        if payload.len() < 11 {
            return Err(anyhow::anyhow!("Invalid payload length: {}", payload.len()));
        }
        if payload[6] == 1 {
            return Ok(Self::at_all());
        }
        let uin = u32::from_be_bytes([payload[7], payload[8], payload[9], payload[10]]);
        Ok(Self::from_at(uin))
    }
}

impl Display for TextElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TextElement::At(uin) => {
                if self.is_at_all() {
                    write!(f, "@全体成员")
                } else {
                    write!(f, "@{}", uin)
                }
            }
        }
    }
}

impl Debug for TextElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

