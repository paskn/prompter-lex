import os
import glob
import math
import pandas as pd

def chunk_df(df, n_chunks, path_dir):
    """
    Split a pandas DataFrame into n_chunks of approximately equal size
    and save each chunk as a CSV file in the specified directory.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to split
    n_chunks : int
        Number of chunks to split the DataFrame into
    path_dir : str
        Path to the directory where CSV files will be saved
    
    Returns:
    --------
    list
        List of file paths where chunks were saved
    """
    # Ensure the directory exists
    os.makedirs(path_dir, exist_ok=True)
    
    # Calculate the size of each chunk
    total_rows = len(df)
    chunk_size = math.ceil(total_rows / n_chunks)
    
    # Store file paths
    saved_files = []
    
    # Split and save the DataFrame
    for i in range(n_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_rows)
        
        # Skip if we've gone beyond the DataFrame size
        if start_idx >= total_rows:
            break
            
        # Extract the chunk
        chunk = df.iloc[start_idx:end_idx]
        
        # Define the file path
        file_path = os.path.join(path_dir, f"chunk_{i+1}.csv")
        
        # Save the chunk to CSV
        chunk.to_csv(file_path, index=False)
        print(f"Chunk {i+1} saved with {len(chunk)} rows to {file_path}")
        
        # Add the file path to the list
        saved_files.append(file_path)
    
    return saved_files


def merge_prompt_chunk(prompt, data_dir, out_dir):
    """
    Merges a prompt string with the contents of each CSV file in data_dir
    and saves the results as markdown files in out_dir.
    
    Parameters:
    -----------
    prompt : str
        The prompt text to prepend to each CSV file's contents
    data_dir : str
        Path to the directory containing CSV files
    out_dir : str
        Path to the directory where output markdown files will be saved
    
    Returns:
    --------
    list
        List of file paths where merged content was saved
    """
    # Ensure the output directory exists
    os.makedirs(out_dir, exist_ok=True)
    
    # Get list of CSV files in the data directory
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    # Store output file paths
    output_files = []
    
    # Process each CSV file
    for csv_file in csv_files:
        # Get the base filename without extension
        base_filename = os.path.basename(csv_file)
        filename_without_ext = os.path.splitext(base_filename)[0]
        
        # Read the CSV file into a string
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        
        # Merge the prompt with the CSV content
        merged_content = prompt + "\n\n" + csv_content
        
        # Create the output file path
        output_file = os.path.join(out_dir, f"{filename_without_ext}.md")
        
        # Save the merged content as a markdown file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(merged_content)
        
        print(f"Merged prompt with {csv_file} and saved to {output_file}")
        output_files.append(output_file)
    
    return output_files    
