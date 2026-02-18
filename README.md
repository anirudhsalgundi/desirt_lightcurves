```mermaid
flowchart TD
    A([📦 DECam FITS Files / CSVs]) --> B

    subgraph SETUP ["Stage 0 — One-time Setup"]
        B["00_organize_data.py\nOrganise raw DECam data\ninto structured format"]
    end

    B --> C[(desirt_database.h5)]

    subgraph PIPELINE ["Main Pipeline"]
        direction TD
        C --> D["01_crossmatch_ztf.py\nCross-match sources\nagainst ZTF alerts"]
        D --> E[(master_database.h5)]

        E --> F["02_plot_lightcurves.py\nGenerate lightcurve\n& cutout plots"]
        E --> G["03_filter_candidates.py\nFilter sources by\nscience criteria ⚙️"]

        G --> H[(candidate_subset.h5)]

        F --> I
        H --> I["04_create_summary.py\nAggregate results into\nHTML summary report"]
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





sample h5 data looks like this:

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
