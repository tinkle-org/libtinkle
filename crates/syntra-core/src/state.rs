//! Six-state connection machine (`SNTR-HS-001`).
//!
//! Exactly `CONNECTED → NEGOTIATING → AUTHENTICATING → ACTIVE →
//! CLOSING → CLOSED` with the transition table in handshake.md §1B.
//! No shortcuts, no extra wire states, no re-handshake on one transport
//! (`SNTR-HS-040`). Suggested names map informatively:
//! `DISCONNECTED` = no transport yet (outside Syntra),
//! `TRANSPORT_ESTABLISHED` = `CONNECTED`, and `ERROR` is not a
//! wire-visible state (failures carry a typed code then move to
//! `CLOSING`/`CLOSED`).

use crate::error::{ErrorCode, SyntraError};

/// Normative Syntra connection states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Transport up, no Syntra bytes exchanged. Handshake timer runs.
    Connected,
    /// `hello` sent; version overlap being evaluated.
    Negotiating,
    /// Version agreed; credential being evaluated.
    Authenticating,
    /// `ready` received; RPCs/cancel/notifications permitted.
    Active,
    /// Graceful close in progress; no new RPCs.
    Closing,
    /// Transport released; per-connection state discarded.
    Closed,
}

impl ConnectionState {
    /// True when application RPCs may be dispatched.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }

    /// True when the connection is terminally closed.
    pub fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Validate a transition; returns the next state or a
    /// `FAILED_PRECONDITION` protocol error.
    pub fn transition(self, event: StateEvent) -> Result<Self, SyntraError> {
        use ConnectionState as S;
        use StateEvent as E;
        let next = match (self, event) {
            (S::Connected, E::HelloSent) => S::Negotiating,
            (S::Negotiating, E::VersionAccepted) => S::Authenticating,
            (S::Authenticating, E::ReadyReceived) => S::Active,
            (S::Connected, E::TransportLost)
            | (S::Negotiating, E::TransportLost)
            | (S::Authenticating, E::TransportLost)
            | (S::Active, E::TransportLost) => S::Closed,
            (S::Active, E::GracefulClose) => S::Closing,
            (S::Connected, E::HandshakeFailed)
            | (S::Negotiating, E::HandshakeFailed)
            | (S::Authenticating, E::HandshakeFailed) => S::Closing,
            (S::Closing, E::TeardownComplete) => S::Closed,
            // ACTIVE-phase protocol violations that close the connection
            // (second hello, hello/ready outside handshake, etc.).
            (S::Active, E::ProtocolViolationFatal) => S::Closing,
            _ => {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::FailedPrecondition,
                    message: format!("illegal state transition: {self:?} + {event:?}"),
                })
            }
        };
        Ok(next)
    }
}

/// Events that drive the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateEvent {
    HelloSent,
    VersionAccepted,
    ReadyReceived,
    HandshakeFailed,
    GracefulClose,
    TransportLost,
    TeardownComplete,
    /// A fatal ACTIVE-phase violation (e.g. `hello` after ACTIVE).
    ProtocolViolationFatal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path() {
        let s = ConnectionState::Connected;
        let s = s.transition(StateEvent::HelloSent).unwrap();
        assert_eq!(s, ConnectionState::Negotiating);
        let s = s.transition(StateEvent::VersionAccepted).unwrap();
        assert_eq!(s, ConnectionState::Authenticating);
        let s = s.transition(StateEvent::ReadyReceived).unwrap();
        assert_eq!(s, ConnectionState::Active);
    }

    #[test]
    fn no_shortcut_connected_to_active() {
        let s = ConnectionState::Connected;
        assert!(s.transition(StateEvent::ReadyReceived).is_err());
    }

    #[test]
    fn loss_closes() {
        let s = ConnectionState::Active;
        assert_eq!(
            s.transition(StateEvent::TransportLost).unwrap(),
            ConnectionState::Closed
        );
    }
}
