//! Vessel imagery: freely licensed photos (Wikimedia Commons) and held,
//! restricted ones for the vessels in the identity registry, with visual
//! attributes (type, colours, funnels, masts, gear) read from each photo by
//! a System 1 model and rolled up into a per-vessel visual profile. See
//! `VESSEL_IMAGERY.md` at the repository root.

pub mod collect;
pub mod commons;
pub mod describe;
pub mod identity;
pub mod images;
pub mod questions;
pub mod systemone;
pub mod tables;
