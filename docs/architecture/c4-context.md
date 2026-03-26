# C4 Context-Level Documentation

## 1. System Overview Section
- **Short Description**: A rapid, data-driven geospatial impact model predicting what exact buildings will be destroyed by an incoming hurricane/tsunami.
- **Long Description**: The American Red Cross requires immediate intelligence on building damages before human assessors can deploy safely. This system ingests FEMA's NSI (National Structure Inventory) arrayed against NOAA SLOSH hydrodynamic models. Within hours of a warning advisory, the platform spins up scalable AWS instances to overlay local properties traversing FEMA's proprietary FAST functions, dumping quantified economic and physical loss estimates to dashboards via Athena queries. 

## 2. Personas Section
### 1. Operations / Shelter Planner (Human)
- **Type**: Human User
- **Description**: Red Cross staff prioritizing areas of mass care response without deep engineering capabilities.
- **Goals**: Decide how many caseworkers to commit and how many shelters to open in targeted counties.
- **Features Used**: Reads aggregate predictions via Excel or data visualization dashboards powered by the database.

### 2. Data Scientist / Pipeline Admin (Programmatic)
- **Type**: Human User
- **Description**: Technical steward maintaining pipeline logic.
- **Goals**: Process new flood rasters, trigger scalable cloud batch nodes.
- **Features Used**: Initiates CLI deployment via `launch_cloud_parallel.sh`.

## 3. External Systems and Dependencies
### 1. NOAA SLOSH & HURREVAC
- **Type**: External Geospatial Agency Feed
- **Description**: Provides the MOM (Maximum of Maximums) advisory water depth arrays per coast.
- **Purpose**: Base prediction factor for water surge.

### 2. USACE NSI (National Structure Inventory)
- **Type**: External Static Dataset
- **Description**: 30 million+ building points mapped nationwide.
- **Purpose**: Establishes what targets are at risk in designated zip codes.

### 3. FEMA FAST Tool
- **Type**: Embedded External Engine
- **Description**: Deterministic physical calculations table and python executable.
- **Purpose**: Converts "2ft of water" into "$200,000 damage and 14 days displacement".

## 4. System Context Diagram
```mermaid
C4Context
    title System Context diagram for ARC Capstone
    
    Person(rc_staff, "Red Cross Planner", "Reviews aggregated damage to deploy mass care")
    Person(ds_admin, "Data Scientist", "Triggers models and feeds advisories")
    
    System(arc_system, "Immediate Impact Modeling System", "Produces property-level loss data intersecting maps and structures via parallel clusters")
    
    SystemExt(noaa, "NOAA National Weather Service", "Provides Tsunami/SLOSH advisory grids")
    SystemExt(fema, "FEMA Source APIs", "Provides original NSI data & FAST curves")
    
    Rel(ds_admin, arc_system, "Manages and Executes Pipeline")
    Rel(rc_staff, arc_system, "Consumes Dashboard Outputs (via Athena)")
    Rel(arc_system, noaa, "Ingests hazard warnings")
    Rel(arc_system, fema, "Ingests building assets")
```

## 5. Related Documentation Links
- [Container Documentation](./c4-container.md)
- [Component Specifications](./c4-component.md)
