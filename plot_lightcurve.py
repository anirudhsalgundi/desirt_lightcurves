import matplotlib.pyplot as plt


# need to use in a for loop for all objects
        def plot_lightcurve(self, object_data, objid):
            """
            Plot lightcurve for a single object
            
            Parameters:
            -----------
            obj_data : dict
                Dictionary containing the object's data
            objid : str
                Object identifier for the title
            """
            # Extract data
            dates_first = obj_data['dates']['first']
            dates_peak = obj_data['dates']['peak']
            dates_last = obj_data['dates']['last']
            
            mags_first = obj_data['mags']['first']
            mags_peak = obj_data['mags']['peak']
            mags_last = obj_data['mags']['last']
            
            errors_first = obj_data['magerrs']['first']
            errors_peak = obj_data['magerrs']['peak']
            errors_last = obj_data['magerrs']['last']
            
            filters_first = obj_data['filters']['first']
            filters_peak = obj_data['filters']['peak']
            filters_last = obj_data['filters']['last']
            
            # Combine all data
            all_dates = dates_first + dates_peak + dates_last
            all_mags = mags_first + mags_peak + mags_last
            all_errors = errors_first + errors_peak + errors_last
            all_filters = filters_first + filters_peak + filters_last
            
            # Convert dates to JD
            if isinstance(all_dates[0], str):
                # Convert ISO strings to JD
                all_dates = [Time(d.replace('Z', '+00:00'), format='isot').jd for d in all_dates]
            else:
                # If already datetime objects
                all_dates = [Time(d).jd for d in all_dates]

            
            t0 = min(all_dates)  # Reference time (e.g., first observation)
            t_now = Time.now().jd
            all_dates = [t_now - t0 for d in all_dates]  # Convert to relative time (days since first observation)

        
            
            # Filter color mapping (common astronomical filters)
            filter_colors = {
                'g': 'green',
                'r': 'red',
                'i': 'orange',
                'z': 'purple',
                'u': 'blue',
                'B': 'blue',
                'V': 'green',
                'R': 'red',
                'I': 'orange'
            }
            
            # Create plot
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Plot by filter
            unique_filters = set(all_filters)
            for filt in unique_filters:
                # Get indices for this filter
                indices = [i for i, f in enumerate(all_filters) if f == filt]
                
                dates_filt = [all_dates[i] for i in indices]
                mags_filt = [float(all_mags[i]) for i in indices]
                errors_filt = [float(all_errors[i]) for i in indices]
                
                color = filter_colors.get(filt, 'gray')
                
                ax.errorbar(dates_filt, mags_filt, yerr=errors_filt,
                        fmt='o', color=color, label=f'Filter {filt}',
                        markersize=8, capsize=3, alpha=0.7)
            
            # Formatting
            ax.set_xlabel('Days since {}'.format(Time(t0, format='jd').iso), fontsize=12)
            ax.set_ylabel('Magnitude', fontsize=12)
            ax.set_title(f'Lightcurve for: {objid}', fontsize=14, fontweight='bold')
            ax.invert_yaxis()  # Magnitudes are inverted (brighter = lower mag)
            ax.invert_xaxis()
            ax.legend(loc='best')
            ax.grid(True, alpha=0.3)
            
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
