//! Definitions shared by the SQL (`track_points.rs`) and the Rust reducer
//! (`reduce.rs`), so the two cannot drift apart.

/// Earth radius in nautical miles (mean).
pub const EARTH_RADIUS_NM: f64 = 3440.065;

/// Two positions this far apart within the same second count as a jump.
pub const SAME_SECOND_JUMP_NM: f64 = 0.05;

/// Reported speed above this (knots) is not a valid AIS speed.
pub const SOG_MAX_VALID_KN: f64 = 102.2;

/// Reported course at or above this (degrees) is not valid (360 = n/a).
pub const COG_MAX_EXCLUSIVE_DEG: f64 = 360.0;

/// Heading above this (degrees) is not valid (511 = n/a).
pub const HEADING_MAX_VALID_DEG: f64 = 359.0;

/// Great-circle distance in nautical miles. Written to mirror the SQL in
/// `track_points::haversine` operation for operation, so results agree.
pub fn haversine_nm(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let a = (((lat2 - lat1).to_radians()) / 2.0).sin().powf(2.0)
        + lat1.to_radians().cos()
            * lat2.to_radians().cos()
            * (((lon2 - lon1).to_radians()) / 2.0).sin().powf(2.0);
    2.0 * EARTH_RADIUS_NM * a.sqrt().min(1.0).asin()
}

/// Whether a latitude/longitude pair is a usable position.
pub fn valid_position(lat: f64, lon: f64) -> bool {
    (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_degree_of_longitude_at_the_equator_is_about_sixty_miles() {
        let d = haversine_nm(0.0, 0.0, 0.0, 1.0);
        assert!((d - 60.04).abs() < 0.05, "{d}");
        assert_eq!(haversine_nm(10.0, 20.0, 10.0, 20.0), 0.0);
    }
}
