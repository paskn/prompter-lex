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
