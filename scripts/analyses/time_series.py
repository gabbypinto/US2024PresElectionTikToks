# %%
import plotnine as p9
import pandas as pd

fullDataFilePath=""
df = pd.read_csv(fullDataFilePath)
date_string_value_counts = df['utc_date_string'].value_counts()

# Convert the Series to a DataFrame
value_counts_df = date_string_value_counts.reset_index()
value_counts_df['utc_date_string'] = pd.to_datetime(value_counts_df['utc_date_string'])

# Find the minimum and maximum dates
min_date = value_counts_df['utc_date_string'].min()
max_date = value_counts_df['utc_date_string'].max()

value_counts_df.columns = ['utc_date_string', 'count']

# Check if value_counts_df is a DataFrame
if not isinstance(value_counts_df, pd.DataFrame):
    raise ValueError("value_counts_df is not a DataFrame")

# Create an annotations DataFrame
annotations = pd.DataFrame({
    'utc_date_string': [
        pd.to_datetime('2024-01-15'), pd.to_datetime('2024-01-21'),pd.to_datetime('2024-02-06'),pd.to_datetime('2024-03-06')
    ],
    'label': ['A','B','C','D'],
    'nudge_y': [20, 40, 20,20],  # Varying nudge values for demonstration
    'nudge_x': -1
})

# Ensure all annotation dates are in value_counts_df
for date in annotations['utc_date_string']:
    if date not in value_counts_df['utc_date_string'].values:
        new_row = pd.DataFrame({'utc_date_string': [date], 'count': [0]})
        value_counts_df = pd.concat([value_counts_df, new_row], ignore_index=True)


# Adjust the nudge values for the 'A' label if it's going out of bounds
annotations.loc[annotations['label'] == 'A', 'nudge_y'] = 2000  # Adjust this value to bring 'A' within bounds
annotations.loc[annotations['label'] == 'B', 'nudge_y'] = 2000  # Adjust this value to bring 'A' within bounds
annotations.loc[annotations['label'] == 'C', 'nudge_y'] = 15000  # Adjust this value to bring 'A' within bounds


# Define a color for each label
color_map = {
    'A': 'red',
    'B': 'red',
    'C': 'red',
    'D':'red'
}

plot = (p9.ggplot(data=value_counts_df, mapping=p9.aes(x='utc_date_string', y='count'))
        + p9.geom_line())

# Add vertical lines and labels one by one
for i, row in annotations.iterrows():
    # Merge the specific row from annotations with value_counts_df to get the 'count' value
    label_data = pd.merge(pd.DataFrame([row]), value_counts_df, on='utc_date_string', how='left')

    # Add vertical line
    plot += p9.geom_vline(xintercept=row['utc_date_string'], linetype="dashed", color="blue")

    # Add label
    plot += p9.geom_label(
        data=label_data, 
        mapping=p9.aes(x='utc_date_string', y='count'), 
        label=row['label'],  # Set label directly
        nudge_y=row['nudge_y'], 
        # nudge_x=row.get('nudge_x', 0),
        nudge_x=row['nudge_x'],  # Use nudge_x if it's set, otherwise default to 0
        color='blue',  # Set label color to blue
        va='bottom', 
        ha='right',
        size=25
    )

# Optionally adjust the y-axis limits to ensure labels fit
# plot += p9.ylim(0, value_counts_df['count'].max() + 50)  # Adjust the 50 to a number that works for your plot


# Rest of the plot setup
plot += (p9.theme(axis_text_x=p9.element_text(angle=45, hjust=1, size=40),
                  axis_text_y=p9.element_text(size=40),
                  panel_background=p9.element_rect(fill='white'),
                  panel_grid_major=p9.element_blank(),
                  panel_grid_minor=p9.element_blank(),
                  axis_line=p9.element_line(color='black'),
                  legend_position='none'))

plot += p9.labs(x="Publication Date", y="Number of Videos")
plot += p9.theme(axis_text_x=p9.element_text(angle=45, hjust=1, size=25),
                 axis_text_y=p9.element_text(size=25),axis_title_x=p9.element_text(size=22),  # Increase x-axis title size
                 axis_title_y=p9.element_text(size=23))
# plot += p9.scale_x_datetime(freq='W', breaks = 'weeks',date_labels='%m-%d-%Y')
# Adjust x-axis labels to weekly

min_date = value_counts_df['utc_date_string'].min()
max_date = "2024-05-25"
weekly_breaks = pd.date_range(start=min_date, end=max_date, freq='W').tolist()


# if min_date not in weekly_breaks:
#     weekly_breaks.insert(0, min_date)

plot += p9.scale_x_datetime(
    breaks=weekly_breaks,
    date_labels='%m-%d-%Y',
)

# if max_date not in weekly_breaks:
#     weekly_breaks.append(max_date)

plot += p9.ylim(0, value_counts_df['count'].max() + 1000)  # Increase the upper limit to a high value
plot += p9.theme(figure_size=(20, 12))  # Adjust the width and height as needed

print(plot)
# Save the plot to a PNG file
plot.save("plot.png", dpi=300)


# %%



