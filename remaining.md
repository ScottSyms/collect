# AIS Types Not Parsed into Typed Rows

Both `ais-parse` and `aisstream-parse` pass these through as `Other`:

| Type | Name | AISStream API Name |
|------|------|--------------------|
| 6 | Binary Addressed Message | `AddressedBinaryMessage` |
| 7 | Binary Acknowledge | `BinaryAcknowledge` |
| 10 | UTC/Date Inquiry | `CoordinatedUTCInquiry` |
| 11 | UTC/Date Response | — |
| 12 | Addressed Safety Related Message | `AddressedSafetyMessage` |
| 13 | Safety Related Acknowledgement | — |
| 14 | Safety Related Broadcast Message | `SafetyBroadcastMessage` |
| 15 | Interrogation | `Interrogation` |
| 16 | Assignment Mode Command | `AssignedModeCommand` |
| 17 | DGNSS Broadcast Binary Message | `GnssBroadcastBinaryMessage` |
| 20 | Data Link Management Message | `DataLinkManagementMessage` |
| 22 | Channel Management | `ChannelManagement` |
| 23 | Group Assignment Command | `GroupAssignmentCommand` |
| 25 | Single Slot Binary Message | `SingleSlotBinaryMessage` |
| 26 | Multi Slot Binary Message | `MultiSlotBinaryMessage` |
| 27 | Long Range AIS Broadcast Message | `LongRangeAisBroadcastMessage` |

## Parsed types

Types 4 (Base Station Report) and 19 (Extended Class B Position Report) are now decoded into `PositionRow` rows in both crates. Type 4 uses `ais_class = "Base Station"` and `nav_status = ""`. Type 19 uses `ais_class = "Class B"` and `nav_status = "under way using engine"` (same as type 18).

## ais-parse specific

Types 4, 11, 14, 15 are recognised by `nmea-parser` and decoded into their corresponding `ParsedMessage` variants. Type 4 is materialised as a `PositionRow`; types 11, 14, 15 fall through to `Decoded::Other`.

Types 6, 7, 10, 12, 13, 16, 17, 20, 22, 23, 25, 26, 27 are not handled by `nmea-parser` and arrive as `ParsedMessage::Unknown` or a parse error.

## aisstream-parse specific

All types above have AISStream API message names listed in `decode_row`'s catch-all `Other` arm. Type 27 (`LongRangeAisBroadcastMessage`) is also in the `Other` arm — unlike ais-parse, it is not decoded into `PositionRow` rows.
