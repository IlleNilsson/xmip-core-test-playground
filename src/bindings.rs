//! The bindings a run places the Playground's Xmip Applications with: each
//! Receive Location and each Send Port the cluster's file holds, on the
//! node and at the address the run gives it — never the file's, which says
//! nothing of where the cluster runs (ADR-0028, amendment 2026-10-21). Written
//! once, for the simulated nodes' process stage (`subscribing::configured`)
//! and for every node the run starts as `xmip-service` (`served`).

/// The two lists of an Application a binding places.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placed {
    /// A Receive Location.
    Receive,
    /// A Send Port.
    Send,
}

impl Placed {
    /// The list's key in a binding.
    #[must_use]
    pub const fn list(self) -> &'static str {
        match self {
            Self::Receive => "receive_locations",
            Self::Send => "send_ports",
        }
    }
}

/// Where one is bound: its node, its file address, and whether a Receive
/// Location accepts what the drop folder's circumstance identifies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bound {
    pub node: String,
    pub address: String,
    pub accepting: bool,
}

/// The bindings of every Application `designed` holds, each Receive
/// Location and Send Port placed where `place` says, over the file
/// transport, as TOML to stand before the cluster's file.
#[must_use]
pub fn written(
    designed: &[configure::XmipApplication],
    place: impl Fn(Placed, &str) -> Bound,
) -> String {
    let mut bindings: Vec<String> = Vec::new();
    for application in designed {
        bindings.push(format!(
            "[[applications]]\nname = {}\n\n",
            quoted(&application.name)
        ));
        let receive = application.receive_locations.iter().map(|one| &one.name);
        let send = application.send_ports.iter().map(|one| &one.name);
        let lists = [
            (Placed::Receive, receive.collect::<Vec<_>>()),
            (Placed::Send, send.collect()),
        ];
        for (placed, names) in lists {
            for name in names {
                let bound = place(placed, name);
                let accept = if bound.accepting {
                    format!(
                        "[applications.{}.accept]\nmechanism = [\"circumstance\"]\n",
                        placed.list()
                    )
                } else {
                    String::new()
                };
                bindings.push(format!(
                    "[[applications.{}]]\nname = {}\nnode = {}\nstart = true\n\
                     transport = \"xmip-core-transport-file\"\naddress = {}\n{accept}\n",
                    placed.list(),
                    quoted(name),
                    quoted(&bound.node),
                    quoted(&bound.address),
                ));
            }
        }
    }
    bindings.concat()
}

/// `text` as a TOML string, quoted and escaped as TOML writes it.
#[must_use]
pub fn quoted(text: &str) -> String {
    toml::Value::String(text.to_string()).to_string()
}
