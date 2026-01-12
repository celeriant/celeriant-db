---
name: excalidraw-tool
description: Provides guidance for creating diagrams using the Excalidraw MCP server. Follow these rules to create clean, organized, and usable diagrams.
---

# Excalidraw Diagram Creation Skill

## Overview

This skill provides guidance for creating diagrams using the Excalidraw MCP server. Follow these rules to create clean, organized, and usable diagrams.

## Available Tools

| Tool | Purpose |
|------|---------|
| `create_from_mermaid` | Convert Mermaid syntax to Excalidraw elements |
| `create_element` | Create a single element |
| `batch_create_elements` | Create multiple elements at once |
| `update_element` | Modify an existing element |
| `delete_element` | Remove an element |
| `query_elements` | Find elements by type or filter |
| `group_elements` | Group elements together for easy movement |
| `ungroup_elements` | Ungroup a group |
| `align_elements` | Align elements (left, center, right, top, middle, bottom) |
| `distribute_elements` | Distribute elements evenly (horizontal, vertical) |
| `lock_elements` | Lock elements to prevent modification |
| `unlock_elements` | Unlock elements |
| `get_resource` | Get scene, library, theme, or elements |

## Critical Rules

### Rule 1: Choose ONE Approach Per Diagram

**NEVER mix Mermaid conversion with manual element creation.**

- **Option A - Mermaid:** Use `create_from_mermaid` for flowcharts, sequences, or graphs where Mermaid syntax is sufficient
- **Option B - Manual:** Use `batch_create_elements` for custom diagrams requiring precise control

```
WRONG:
1. create_from_mermaid → creates diagram
2. create_element → adds box on top of mermaid output  ❌

CORRECT:
1. create_from_mermaid → done  ✓
   OR
1. batch_create_elements → creates all elements  ✓
```

### Rule 2: Always Group Related Elements

After creating elements that form a logical section, **immediately group them** using `group_elements`. This allows users to move sections as a unit.

```
Example workflow:
1. batch_create_elements → creates "Auth Flow" section (3 boxes + 2 arrows)
2. query_elements → get IDs of the created elements
3. group_elements → group them together with a descriptive purpose
4. Repeat for next section
```

**Grouping guidelines:**
- Group elements that represent a single concept or flow
- Group labels with their associated shapes
- Create hierarchical groups for complex diagrams (sub-groups within larger groups)

### Rule 3: Calculate Positions Explicitly

Never guess positions. Always calculate coordinates to prevent overlap.

**Standard spacing:**
- Horizontal gap between elements: 150-200px
- Vertical gap between elements: 100-150px
- Padding within sections: 50px

**Position calculation pattern:**
```
Section 1: x=0, y=0
  Element 1: x=50, y=50, width=150, height=80
  Element 2: x=250, y=50, width=150, height=80  (50 + 150 + 50 gap)

Section 2: x=0, y=250 (below section 1 with 100px gap)
  Element 3: x=50, y=300
```

### Rule 4: Use Layout Tools for Polish

After creating elements, use alignment and distribution tools:

1. `align_elements` - Align related elements:
   - `left`, `center`, `right` for horizontal alignment
   - `top`, `middle`, `bottom` for vertical alignment

2. `distribute_elements` - Space elements evenly:
   - `horizontal` - Equal horizontal spacing
   - `vertical` - Equal vertical spacing

### Rule 5: Use batch_create_elements for Multiple Elements

**Never call `create_element` multiple times in sequence.** Use `batch_create_elements` to create all elements in a single operation.

```
WRONG:
create_element → box 1
create_element → box 2
create_element → arrow 1  ❌

CORRECT:
batch_create_elements → [box1, box2, arrow1]  ✓
```

### Rule 6: Prefer directional arrows

Creating space between boxes / nodes and linking with a directional arrow is preferred. Always try to model things as a directed acyclic graph if possible. Do not create arrows that do not *anchor* to other shapes, as when they are moved, the arrow doesn't move with it.

## Element Types Reference

| Type | Use Case | Key Properties |
|------|----------|----------------|
| `rectangle` | Boxes, containers, process steps | width, height, backgroundColor |
| `ellipse` | Start/end nodes, circular elements | width, height |
| `diamond` | Decision points, conditionals | width, height |
| `arrow` | Connections, flow direction | - |
| `line` | Connectors without direction | - |
| `text` | Standalone labels | text, fontSize, fontFamily |
| `label` | Labels attached to shapes | text, fontSize |

## Standard Styling

**Colors (use consistently):**
- Primary boxes: `#a5d8ff` (light blue)
- Secondary boxes: `#d0bfff` (light purple)
- Success/start: `#b2f2bb` (light green)
- Warning/decision: `#ffec99` (light yellow)
- Error/end: `#ffc9c9` (light red)
- Neutral: `#e9ecef` (light gray)

**Stroke and text:**
- strokeColor: `#000000` or `#1e1e1e`
- strokeWidth: 1 or 2
- fontSize: 16 for labels, 20 for titles
- fontFamily: 1 (hand-drawn), 2 (normal), 3 (monospace)

**Fill**
- prefer hatched over full fill for background shape fill

## Workflow Templates

### Template: Flowchart (Manual)

```
1. Plan the layout:
   - Identify number of rows and columns
   - Calculate total width and height
   - Determine spacing

2. Create all elements with batch_create_elements:
   - All boxes/shapes first
   - All arrows/connectors
   - All labels

3. Query elements to get IDs

4. Group logical sections:
   - Group each "lane" or "phase"
   - Group related decision branches

5. Apply alignment:
   - Align boxes in same row horizontally
   - Align boxes in same column vertically

6. Distribute if needed:
   - Distribute rows/columns evenly
```

### Template: Architecture Diagram

```
1. Define sections (e.g., Frontend, Backend, Database)

2. For each section:
   a. Create container rectangle (large, light background)
   b. Create internal elements
   c. Create section title label
   d. Group all section elements

3. Create connections between sections

4. Align section containers

5. Add legend if needed (grouped separately)
```

### Template: Simple Diagram via Mermaid

```
1. Write Mermaid syntax for the diagram
2. Call create_from_mermaid with the syntax
3. Query elements to get created element IDs
4. Group logical sections if needed
5. Done - do NOT add manual elements
```

## Common Mistakes to Avoid

1. **Overlapping elements** - Always calculate positions based on element dimensions
2. **Forgetting to group** - Users cannot easily rearrange ungrouped elements
3. **Mixing approaches** - Mermaid + manual = overlapping mess
4. **Sequential create_element calls** - Use batch_create_elements instead
5. **Hardcoded positions without spacing** - Use consistent gaps
6. **Missing labels** - Always label shapes for clarity
7. **Inconsistent styling** - Use the standard color palette

## Pre-Creation Checklist

Before creating any diagram, confirm:

- [ ] Chosen approach: Mermaid OR Manual (not both)
- [ ] Planned layout with explicit coordinates
- [ ] Identified logical groups
- [ ] Selected consistent color scheme
- [ ] Calculated spacing to prevent overlap
