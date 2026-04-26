# TUI (Terminal User Interface) Guide

> Legacy documentation for the pre-workspace single-binary layout. The current workspace exposes the same TUI through `collect-file --tui` and `collect-socket --tui`.

## Overview

The current `collect-file` and `collect-socket` binaries include an interactive Terminal User Interface (TUI) for easy configuration of all available options. The TUI provides a tab-based interface with validation, hints, and config file support.

## Key Features

- ✅ **5 Configuration Tabs**: Input, Output, S3, Config, Run
- ✅ **Field Validation**: Automatic validation before running
- ✅ **Helpful Hints**: Press `?` to see examples for each field
- ✅ **Save/Load Configs**: Export and reuse configurations
- ✅ **Environment Loading**: Auto-loads values from environment variables
- ✅ **Real-time Feedback**: Status messages and error display

## Launching the TUI

```bash
cargo run -p collect-file -- --tui
```

The TUI will automatically load values from:
1. Environment variables (if set)
2. CLI arguments (if provided before --tui)

## Navigation

### Tab Navigation
- **Tab** - Move to next tab
- **Shift+Tab** - Move to previous tab

### Field Navigation
- **↑ / ↓** - Navigate between fields in current tab
- **Enter** - Edit selected field (or run from Run tab)
- **Space** - Toggle boolean fields (checkboxes)

### Editing
- **Type** - Enter text for the field
- **Backspace** - Delete characters
- **Enter** - Save changes
- **Esc** - Cancel editing

### General
- **q** - Quit without running
- **?** or **F1** - Toggle help/hints display
- **v** - Validate configuration

## Tab Details

### 1. Input Tab
Configure the data source:
- **Input File**: Path to text file (one record per line)
- **TCP Host**: Remote TCP server address
- **TCP Port**: TCP port number
- **Source Label**: Logical name for this data source

*Note: Only one input method should be configured (File OR TCP)*

### 2. Output Tab
Configure local file storage:
- **Output Directory**: Where to write Parquet files (default: `data`)
- **Max Rows per File**: Optional limit before flushing to new file
- **Keep Local Files**: [X] to retain files after S3 upload

### 3. S3 Tab
Configure cloud storage:
- **S3 Bucket**: Bucket name for uploads
- **S3 Endpoint**: Custom endpoint (for MinIO, R2, etc.)
- **S3 Region**: AWS region (default: us-east-1)
- **S3 Key Prefix**: Prefix for S3 object keys
- **S3 Access Key**: AWS access key ID
- **S3 Secret Key**: AWS secret access key (masked)
- **Disable TLS**: [X] to use HTTP instead of HTTPS

### 4. Config Tab
Save and load configurations:
- **Config File Path**: Path to JSON config file (default: `collect-file-config.json` for the file binary, `collect-socket-config.json` for the socket binary)
- **Save Config**: Press Enter to save current settings to file
- **Load Config**: Press Enter to load settings from file

*Saved configs can be version controlled or shared with team members.*

### 5. Run Tab
Review the final configuration and launch:
- Displays the full command line that will be executed
- **Automatic validation** runs before execution
- Validation errors are displayed if any issues found
- Press **Enter** to start (only if validation passes)
- Press **q** to quit without running

## Validation

The TUI includes automatic validation that checks:

- ✅ At least one input source is configured
- ✅ Only one input source is active (no conflicts)
- ✅ Required fields are present (e.g., TCP port with TCP host)
- ✅ Numeric fields contain valid numbers
- ✅ S3 credentials available (fields or environment)

**Validation is automatic** when you try to run, or press `v` to validate manually at any time.

## Help System

Press `?` or `F1` to toggle the help/hints display:

- Shows **example values** for each field
- Updates as you navigate between fields
- Press `?` again to hide hints and maximize space

Examples:
- Input File: `e.g., /path/to/data.txt (one record per line)`
- TCP Port: `e.g., 5631`

## Config File Management

### Saving Configurations

1. Navigate to **Config** tab
2. Edit **Config File Path** if desired (default: `collect-file-config.json` for the file binary, `collect-socket-config.json` for the socket binary)
3. Select **Save Config** and press Enter
4. Status message confirms save: `✓ Config saved to collect-file-config.json`

Saved configs are JSON files containing all settings:
```json
{
  "input_file": "/path/to/data.txt",
  "source": "my-source",
  "out_dir": "data",
  "s3_bucket": "my-bucket",
  ...
}
```

### Loading Configurations

1. Navigate to **Config** tab
2. Edit **Config File Path** to point to saved config
3. Select **Load Config** and press Enter
4. All fields update with loaded values
5. Status message confirms: `✓ Config loaded from collect-file-config.json`

### Example Workflow

```bash
# Session 1: Configure and save
cargo run -p collect-file -- --tui
# Configure settings, save to production-config.json

# Session 2: Load and run
cargo run -p collect-file -- --tui
# Load production-config.json, verify, run
```

## Examples

### Basic Usage
Start TUI with no preset values:
```bash
cargo run -p collect-file -- --tui
```

### Pre-populated from Environment
Set environment variables first:
```bash
export S3_BUCKET=my-bucket
export S3_REGION=us-west-2
export TCP_HOST=153.44.253.27
export TCP_PORT=5631
cargo run -p collect-file -- --tui
```

The TUI will show these values pre-filled.

### Mix CLI and TUI
Provide some options via CLI, configure rest in TUI:
```bash
cargo run -p collect-file -- --out-dir /data/output --s3-bucket my-bucket --tui
```

## Tips

1. **Boolean Fields**: Use Space or Enter to toggle checkboxes
2. **Numeric Fields**: Ensure ports and max rows are valid numbers
3. **Passwords**: Secret keys are masked with `***` in display but are preserved
4. **Validation**: Press `v` anytime to check your configuration before running
5. **Hints**: Press `?` to see examples for each field as you navigate
6. **Config Files**: Save frequently-used configurations for quick reuse
7. **Environment**: TUI reads from environment variables automatically
8. **Status Messages**: Watch the status bar for save/load confirmations
9. **Error Display**: Validation errors appear in red above the footer

## Keyboard Shortcuts Summary

| Key | Action |
|-----|--------|
| Tab | Next tab |
| Shift+Tab | Previous tab |
| ↑ / ↓ | Navigate fields |
| Enter | Edit field / Run / Save/Load config |
| Space | Toggle checkbox |
| Esc | Cancel edit |
| ? / F1 | Toggle help/hints |
| v | Validate configuration |
| q | Quit |

## Troubleshooting

### TUI won't start
- Ensure terminal supports ANSI escape codes
- Try running in a different terminal (iTerm2, Terminal.app, etc.)

### Display issues
- Resize terminal window to at least 80x24 characters
- Check terminal's Unicode support for proper rendering

### Values not loading
- Verify environment variables are set: `env | grep -E "(S3|TCP)_"`
- Check that CLI arguments are before the `--tui` flag

## Advanced: Scripting

You can programmatically generate configurations by setting environment variables:

```bash
#!/bin/bash
export S3_BUCKET=production-bucket
export S3_REGION=us-east-1
export TCP_HOST=153.44.253.27
export TCP_PORT=5631

# Launch TUI with pre-configured values
cargo run -p collect-file -- --tui
```
