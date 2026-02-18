```mermaid
flowchart TD
    %% Node Definitions
    Raw([📦 DECam Raw Data]):::input
    Setup[00_organize_data.py]:::script
    DB1[(desirt_database.h5)]:::database
    
    Cross[01_crossmatch_ztf.py]:::script
    DB2[(master_database.h5)]:::database
    
    Plot[02_plot_lightcurves.py]:::script
    Filter[03_filter_candidates.py]:::filter
    DB3[(candidate_subset.h5)]:::database
    
    Report[04_create_summary.py]:::script
    Final([🌐 summary.html]):::output

    %% Flow
    Raw --> Setup
    Setup --> DB1
    
    subgraph Pipeline [Main Processing Pipeline]
        direction TB
        DB1 --> Cross
        Cross --> DB2
        DB2 --> Plot
        DB2 --> Filter
        Filter --> DB3
    end

    Plot --> Report
    DB3 --> Report
    Report --> Final

    %% Aesthetic Styling
    classDef script fill:#f9f9f9,stroke:#333,stroke-width:1px,color:#333
    classDef database fill:#e1f5fe,stroke:#01579b,stroke-width:1px,color:#01579b
    classDef filter fill:#fff3e0,stroke:#e65100,stroke-width:1px,color:#e65100
    classDef input fill:#eceff1,stroke:#455a64,stroke-width:2px
    classDef output fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,stroke-dasharray: 5 5
    
    style Pipeline fill:#fcfcfc,stroke:#ddd,stroke-dasharray: 5 5,color:#999
```

```mermaid
flowchart TD
    A([📦 DECam FITS Files / CSVs]) --> B

    subgraph SETUP ["Stage 0 — One-time Setup"]
        B["00_organize_data.py | Organise raw DECam data into structured format"]
    end

    B --> C[(desirt_database.h5\nra, dec, mjds, filters\nmag_fphot, mag_alt\nscience/diff/template images)]

    subgraph PIPELINE ["Main Pipeline"]
        C --> D["01_crossmatch_ztf.py | Cross-match sources against ZTF alerts"]
        D --> E[(master_database.h5)]
        E --> F["02_plot_lightcurves.py | Generate lightcurve and cutout plots"]
        E --> G["03_filter_candidates.py | Filter sources by science criteria ⚙️"]
        G --> H[(candidate_subset.h5)]
        F --> I["04_create_summary.py | Aggregate results into HTML summary report"]
        H --> I
    end

    I --> J([🌐 summary.html])

    style SETUP fill:#1e1e2e,stroke:#6c7086,color:#cdd6f4
    style PIPELINE fill:#1e1e2e,stroke:#6c7086,color:#cdd6f4
    style A fill:#313244,stroke:#89b4fa,color:#cdd6f4
    style J fill:#313244,stroke:#a6e3a1,color:#cdd6f4
    style C fill:#313244,stroke:#f38ba8,color:#cdd6f4
    style E fill:#313244,stroke:#f38ba8,color:#cdd6f4
    style H fill:#313244,stroke:#fab387,color:#cdd6f4
    style G fill:#45475a,stroke:#fab387,color:#fab387
```




<details>
<summary> HDF5 Database Schema</summary>
A sample HDF5 file structure for a single source in the DESIRT master database is shown below. Each source is stored as a group named by its coordinates (e.g., `/A202502031447311m004707`), containing attributes for RA and Dec, and datasets for lightcurve data and image cutouts.
```bash
[salgundi@bridges2-login014 results]$ h5dump -A -g /A202502031447311m004707 desirt_master_database_20260217_184644.h5
HDF5 "desirt_master_database_20260217_184644.h5" {
GROUP "/A202502031447311m004707" {
   ATTRIBUTE "dec" {
      DATATYPE  H5T_IEEE_F64LE
      DATASPACE  SCALAR
      DATA {
      (0): -0.785369
      }
   }
   ATTRIBUTE "ra" {
      DATATYPE  H5T_IEEE_F64LE
      DATASPACE  SCALAR
      DATA {
      (0): 221.88
      }
   }
   DATASET "difference_image" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 48, 121, 121 ) / ( 48, 121, 121 ) }
   }
   DATASET "filters" {
      DATATYPE  H5T_STRING {
         STRSIZE 1;
         STRPAD H5T_STR_NULLPAD;
         CSET H5T_CSET_ASCII;
         CTYPE H5T_C_S1;
      }
      DATASPACE  SIMPLE { ( 48 ) / ( 48 ) }
   }
   DATASET "mag_alt" {
      DATATYPE  H5T_IEEE_F64LE
      DATASPACE  SIMPLE { ( 48 ) / ( 48 ) }
   }
   DATASET "mag_fphot" {
      DATATYPE  H5T_IEEE_F64LE
      DATASPACE  SIMPLE { ( 48 ) / ( 48 ) }
   }
   DATASET "magerr_alt" {
      DATATYPE  H5T_IEEE_F64LE
      DATASPACE  SIMPLE { ( 48 ) / ( 48 ) }
   }
   DATASET "magerr_fphot" {
      DATATYPE  H5T_IEEE_F64LE
      DATASPACE  SIMPLE { ( 48 ) / ( 48 ) }
   }
   DATASET "mjds" {
      DATATYPE  H5T_IEEE_F64LE
      DATASPACE  SIMPLE { ( 48 ) / ( 48 ) }
   }
   DATASET "science_image" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 48, 121, 121 ) / ( 48, 121, 121 ) }
   }
   DATASET "template_image" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 48, 121, 121 ) / ( 48, 121, 121 ) }
   }
}
}
```
</details>