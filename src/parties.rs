//! The Parties the Playground trades with, and which transports each is on
//! (ADR-0019; ADR-0028, amendments 2026-10-06 and 2026-10-13).
//!
//! The owner, 2026-10-13, watching a run that had one: *You have not
//! Implementet more Parties than Party-X.* The Parties are the Playground's
//! configuration's `[[parties]]`, read by `configure`'s one reading of them
//! (`configure::party`); nothing here names one. The [`Spread`] puts each
//! on its share of the transports in both directions: a Party sends into
//! the Receive Locations of every transport whose place in the list falls
//! to it, and is delivered to over the transports of the next Party's share,
//! so no Party sends and receives over the same transports and every Party
//! is on both sides. Each stream of a transport — each content contract — is
//! that Party's. The identity a Party presents is its first.

use std::sync::OnceLock;

use configure::PartyConfiguration;

use crate::exchange::{RoundTrip, all_transports};
use crate::subscribing::CLUSTER;

/// The Parties a configuration declares, sound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parties {
    parties: Vec<PartyConfiguration>,
}

impl Parties {
    /// The Parties `source` declares, by `configure`'s reading.
    ///
    /// # Errors
    /// When the text does not read, a Party is unsound, or none is declared:
    /// the Playground has no far end without one.
    pub fn of(source: &str) -> Result<Self, String> {
        let parties = configure::party::parties(source)?;
        let problems = configure::party::problems(&parties);
        if !problems.is_empty() {
            return Err(problems.join("; "));
        }
        if parties.is_empty() {
            return Err("the configuration declares no [[parties]]".to_string());
        }
        Ok(Self { parties })
    }

    /// The Parties of the Playground's own configuration, read once.
    ///
    /// # Panics
    /// When the shipped configuration declares none or an unsound one;
    /// `the_shipped_configuration_declares_sound_parties` holds it.
    #[must_use]
    pub fn configured() -> &'static Self {
        static READ: OnceLock<Parties> = OnceLock::new();
        READ.get_or_init(|| Self::of(CLUSTER).expect("the shipped [[parties]] read"))
    }

    /// Every Party, in the order written.
    #[must_use]
    pub fn all(&self) -> &[PartyConfiguration] {
        &self.parties
    }

    /// Their names, in the order written.
    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.parties
            .iter()
            .map(|party| party.name.as_str())
            .collect()
    }

    /// The place of the Party that holds the identity `presented`, if one
    /// does: what a Receive Location resolves a proven claim to.
    #[must_use]
    pub fn holding(&self, presented: &str) -> Option<usize> {
        self.parties
            .iter()
            .position(|party| party.identities.iter().any(|one| one == presented))
    }

    /// The Party at `place`.
    #[must_use]
    pub fn at(&self, place: usize) -> &PartyConfiguration {
        &self.parties[place % self.parties.len()]
    }

    /// These Parties spread over `transports`, in that order.
    #[must_use]
    pub fn spread<S: AsRef<str>>(&self, transports: &[S]) -> Spread {
        Spread {
            parties: self.clone(),
            transports: transports
                .iter()
                .map(|one| one.as_ref().to_string())
                .collect(),
        }
    }

    /// These Parties spread over every transport the Playground drives.
    #[must_use]
    pub fn over_every_transport(&self) -> Spread {
        let names: Vec<&str> = all_transports(std::env::temp_dir())
            .iter()
            .map(|transport| transport.transport())
            .collect();
        self.spread(&names)
    }
}

/// The configuration's Parties spread over `transports`, in their order:
/// what a schedule or a relay judges each pair's Party by.
#[must_use]
pub fn spread_over(transports: &[Box<dyn RoundTrip>]) -> Spread {
    let names: Vec<&str> = transports.iter().map(|one| one.transport()).collect();
    Parties::configured().spread(&names)
}

/// Which Party is on which transport, on each side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spread {
    parties: Parties,
    transports: Vec<String>,
}

impl Spread {
    /// The Parties spread.
    #[must_use]
    pub const fn parties(&self) -> &Parties {
        &self.parties
    }

    /// The transports spread over, in order.
    #[must_use]
    pub fn transports(&self) -> &[String] {
        &self.transports
    }

    fn place(&self, transport: &str) -> usize {
        self.transports
            .iter()
            .position(|one| one == transport)
            .unwrap_or_default()
    }

    /// The place of the Party that sends into the Receive Locations of
    /// `transport`.
    #[must_use]
    pub fn sender(&self, transport: &str) -> usize {
        self.place(transport) % self.parties.parties.len()
    }

    /// The place of the Party the Send Locations of `transport` deliver to:
    /// the next Party's, so a Party receives over other transports than it
    /// sends over.
    #[must_use]
    pub fn receiver(&self, transport: &str) -> usize {
        (self.place(transport) + 1) % self.parties.parties.len()
    }

    /// The transports the Party at `place` sends over.
    #[must_use]
    pub fn sends(&self, place: usize) -> Vec<String> {
        self.over(place, |transport| self.sender(transport))
    }

    /// The transports the Party at `place` is delivered to over.
    #[must_use]
    pub fn receives(&self, place: usize) -> Vec<String> {
        self.over(place, |transport| self.receiver(transport))
    }

    fn over(&self, place: usize, whose: impl Fn(&str) -> usize) -> Vec<String> {
        self.transports
            .iter()
            .filter(|transport| whose(transport) == place)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_configuration_declares_sound_parties() {
        let parties = Parties::of(CLUSTER).expect("sound");
        assert!(
            parties.all().len() > 1,
            "several Parties (ADR-0028, 2026-10-06)"
        );
        assert_eq!(&parties, Parties::configured());
        for (place, party) in parties.all().iter().enumerate() {
            assert_eq!(parties.holding(&party.identities[0]), Some(place));
        }
        assert_eq!(parties.holding("CN=nobody"), None);
        assert!(
            Parties::of("[service]\nname = \"n\"\n").is_err(),
            "none is refused"
        );
    }

    #[test]
    fn every_party_is_on_both_sides_over_several_transports_never_the_same() {
        let spread = Parties::configured().over_every_transport();
        let count = spread.parties().all().len();
        let mut sent = Vec::new();
        for place in 0..count {
            let (sends, receives) = (spread.sends(place), spread.receives(place));
            assert!(
                sends.len() > 1 && receives.len() > 1,
                "{place}: several each way"
            );
            assert!(sends.iter().all(|transport| !receives.contains(transport)));
            sent.extend(sends);
        }
        sent.sort();
        let mut every = spread.transports().to_vec();
        every.sort();
        assert_eq!(sent, every, "each transport has one sending Party");
    }

    #[test]
    fn a_spread_is_by_place_in_the_list_of_transports() {
        let parties = Parties::configured();
        let spread = parties.spread(&["a", "b", "c"]);
        assert_eq!(
            (spread.sender("a"), spread.receiver("a")),
            (0, 1 % parties.all().len())
        );
        assert_eq!(spread.sender("c"), 2 % parties.all().len());
        assert_eq!(spread.sends(0)[0], "a");
    }
}
