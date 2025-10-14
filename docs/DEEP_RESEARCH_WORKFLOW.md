# Deep Research Integration Workflow

## Overview

The Deep Research integration system allows you to enrich facility data using Gemini Deep Research or other LLM-based research tools. The workflow is designed to be iterative, auditable, and preserves data lineage.

## Quick Start

### 1. Generate Research Prompts

```bash
# Generate a prompt for platinum facilities in South Africa
python scripts/deep_research_integration.py \
    --generate-prompt \
    --country ZAF \
    --metal platinum \
    --limit 50

# Output saved to: output/research_prompts/prompt_platinum_ZAF_[timestamp].txt
```

### 2. Submit to Gemini Deep Research

Copy the generated prompt and submit it to Gemini Deep Research. Request the output in JSON format as specified in the prompt.

### 3. Process Research Results

```bash
# Process a single research output file
python scripts/deep_research_integration.py \
    --process research_output.json \
    --country ZAF \
    --metal platinum

# Process batch results (JSONL format)
python scripts/deep_research_integration.py \
    --batch research_batch.jsonl
```

## Data Flow

```
1. Existing Facilities           2. Research Prompt
   facilities/            →      Generate prompt with
   [country]/[id].json           existing facility data
           ↓                              ↓
                                 3. Gemini Deep Research
                                    - Status updates
                                    - Ownership data
                                    - Production capacity
                                    - Sources/citations
                                          ↓
4. Research Output               5. Integration Script
   JSON/JSONL with        →      Merge research data
   enriched facility data        with existing facilities
           ↓                              ↓
6. Updated Facilities            7. Audit Trail
   Enhanced JSON files           research_raw/
   with new data                 research_evidence/
```

## Research Data Format

### Input Format (from Gemini)

The Deep Research output should follow this JSON structure:

```json
[
  {
    "facility_id": "zaf-rustenburg-karee-fac",
    "name": "Karee Mine",
    "status": "operating",
    "owners": [
      {
        "name": "Impala Platinum Holdings",
        "percentage": 74.0,
        "role": "owner",
        "confidence": 0.95
      },
      {
        "name": "Royal Bafokeng Platinum",
        "percentage": 26.0,
        "role": "owner",
        "confidence": 0.95
      }
    ],
    "operator": {
      "name": "Impala Platinum",
      "confidence": 0.95
    },
    "products": [
      {
        "stream": "PGM concentrate",
        "capacity": 250000,
        "unit": "oz",
        "year": 2024
      }
    ],
    "sources": [
      {
        "type": "web",
        "url": "https://www.implats.co.za/operations",
        "date": "2024-10-01"
      }
    ],
    "confidence": 0.9,
    "notes": "Part of Impala Rustenburg complex"
  }
]
```

### Key Fields

- **facility_id**: Must match existing facility ID
- **name**: Alternative lookup if ID not provided
- **status**: `operating`, `closed`, `care_and_maintenance`, `suspended`, `planned`, `construction`
- **owners**: List of companies with ownership stakes
- **operator**: Company operating the facility
- **products**: Production streams with capacities
- **sources**: URLs or references for verification
- **confidence**: Overall confidence score (0.0-1.0)

## Integration Features

### 1. Company Resolution

The integration script automatically:
- Resolves company names to canonical IDs using entityidentity
- Matches against LEI codes and Wikidata entries
- Creates consistent company references across facilities

### 2. Data Preservation

- Original facility data is backed up before updates
- All raw research outputs are saved in `output/research_raw/`
- Update logs track what changed and when

### 3. Verification Status

Facilities are automatically updated with verification status:
- `csv_imported` → `llm_suggested` (after Deep Research)
- Confidence scores are calculated based on source quality
- Timestamps and attribution are maintained

## Workflow Examples

### Example 1: Research Aluminum Facilities in Canada

```bash
# Step 1: Generate prompt
python scripts/deep_research_integration.py \
    --generate-prompt \
    --country CAN \
    --metal aluminum \
    --limit 30

# Step 2: Copy prompt content
cat output/research_prompts/prompt_aluminum_CAN_*.txt

# Step 3: Submit to Gemini Deep Research
# (Manual step - paste prompt and get JSON response)

# Step 4: Save Gemini output as aluminum_canada_research.json

# Step 5: Process the research
python scripts/deep_research_integration.py \
    --process aluminum_canada_research.json \
    --country CAN \
    --metal aluminum

# Step 6: Verify updates
cat facilities/CAN/can-kitimat-smelter-fac.json | jq .
```

### Example 2: Batch Processing Multiple Countries

```bash
# Create a batch JSONL file with research for multiple facilities
cat > batch_research.jsonl << 'EOF'
{"facility_id": "usa-stillwater-mine-fac", "status": "operating", "operator": {"name": "Sibanye-Stillwater"}}
{"facility_id": "zaf-mogalakwena-fac", "status": "operating", "owner": [{"name": "Anglo American Platinum", "percentage": 100}]}
EOF

# Process the batch
python scripts/deep_research_integration.py --batch batch_research.jsonl

# Check the report
cat deep_research_integration.log
```

### Example 3: Finding Facilities by Name

If Gemini returns facility names instead of IDs:

```json
{
  "name": "Grasberg",
  "status": "operating",
  "operator": {
    "name": "PT Freeport Indonesia"
  }
}
```

The script will:
1. Search for facilities matching "Grasberg"
2. Filter by country if provided
3. Filter by metal if provided
4. Update the matching facility

## Best Practices

### 1. Start Small
- Begin with a single country/metal pair
- Verify results before scaling up
- Adjust prompts based on response quality

### 2. Provide Context
When generating prompts, include:
- Existing facility names and aliases
- Geographic coordinates for verification
- Current commodity focus

### 3. Request Sources
Always ask Gemini to provide:
- URLs for all facts
- Dates for time-sensitive information
- Confidence levels for uncertain data

### 4. Iterative Refinement
- Review updated facilities after integration
- Identify gaps or errors
- Re-run research for specific facilities

### 5. Data Quality Checks

```bash
# Check facilities with high confidence
find facilities -name "*.json" -exec grep -l '"confidence": 0.9' {} \;

# Find facilities still needing research
find facilities -name "*.json" -exec grep -l '"status": "unknown"' {} \;

# Count facilities by status
for status in operating closed suspended; do
  echo "$status: $(grep -r "\"status\": \"$status\"" facilities | wc -l)"
done
```

## Monitoring Progress

### Check Integration Statistics

```python
# Python script to analyze facility enrichment
import json
from pathlib import Path

facilities_dir = Path("facilities")
stats = {
    "total": 0,
    "with_status": 0,
    "with_owners": 0,
    "with_operator": 0,
    "with_products": 0
}

for facility_file in facilities_dir.glob("**/*.json"):
    with open(facility_file) as f:
        facility = json.load(f)

    stats["total"] += 1
    if facility.get("status") != "unknown":
        stats["with_status"] += 1
    if facility.get("owner_links"):
        stats["with_owners"] += 1
    if facility.get("operator_link"):
        stats["with_operator"] += 1
    if facility.get("products"):
        stats["with_products"] += 1

print(f"Total facilities: {stats['total']}")
print(f"With status: {stats['with_status']} ({100*stats['with_status']/stats['total']:.1f}%)")
print(f"With owners: {stats['with_owners']} ({100*stats['with_owners']/stats['total']:.1f}%)")
print(f"With operator: {stats['with_operator']} ({100*stats['with_operator']/stats['total']:.1f}%)")
print(f"With products: {stats['with_products']} ({100*stats['with_products']/stats['total']:.1f}%)")
```

### View Recent Updates

```bash
# Check modification times
find facilities -name "*.json" -mtime -1 | head -20

# View update log
tail -50 deep_research_integration.log
```

## Error Handling

### Common Issues and Solutions

1. **Facility Not Found**
   - Check facility_id matches exactly
   - Try searching by name instead
   - Verify country code is correct

2. **Company Resolution Failed**
   - Company name might need manual mapping
   - Add to config/mappings/company_canonical.json
   - Re-run with updated mappings

3. **Invalid JSON from Gemini**
   - Validate JSON structure
   - Check for unescaped quotes
   - Use a JSON validator/formatter

4. **Confidence Too Low**
   - Request more specific sources
   - Ask for primary sources only
   - Focus on official company websites

## Output Files

### Updated Facilities
- **Location**: `facilities/[country]/[facility-id].json`
- **Content**: Enhanced facility data with research results

### Raw Research Archive
- **Location**: `output/research_raw/[metal]_[country]_[timestamp].json`
- **Content**: Original research output for audit trail

### Research Prompts
- **Location**: `output/research_prompts/prompt_[metal]_[country]_[timestamp].txt`
- **Content**: Generated prompts for Deep Research

### Integration Logs
- **Location**: `deep_research_integration.log`
- **Content**: Detailed processing logs

### Backup Files
- **Location**: Same directory as facility, with `.backup_[timestamp].json` extension
- **Content**: Original facility data before updates

## Next Steps

1. **Priority Metals**: Start with high-value metals (gold, platinum, copper)
2. **Major Countries**: Focus on top producing countries first
3. **Verification**: Manual review of high-importance facilities
4. **Automation**: Set up scheduled research updates
5. **Quality Metrics**: Track confidence scores and source quality

## Advanced Usage

### Custom Research Templates

Create specialized prompts for specific scenarios:

```python
# Generate targeted research for specific facility types
facilities = load_facilities_by_type("smelter")
prompt = generate_smelter_research_prompt(facilities)
```

### Incremental Updates

Update only facilities that need research:

```bash
# Find facilities without operators
for file in $(grep -l '"operator_link": null' facilities/**/*.json); do
  facility_id=$(basename $file .json)
  echo "Needs operator: $facility_id"
done
```

### Source Validation

Verify research sources are recent and authoritative:

```python
# Check source dates
for source in facility['sources']:
    if source['type'] == 'web':
        date = source.get('date')
        if date and is_older_than_year(date):
            print(f"Outdated source: {source['url']}")
```

## Support

For issues or questions about the Deep Research integration:
1. Check the integration logs
2. Verify JSON format matches schema
3. Review this documentation
4. Check facility.schema.json for field requirements