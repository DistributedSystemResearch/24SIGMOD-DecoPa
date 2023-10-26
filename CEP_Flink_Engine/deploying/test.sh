DIRECTORY=

# Loop over all folders in the directory
for FOLDER in "$DIRECTORY"/*/; do
  for SUBFOLDER in "$FOLDER"*/; do
    # Run the ./run script in the background and save its PID in a variable
    ./run "$SUBFOLDER" 0 5 &
    RUN_PID=$!

    # Sleep for 15 minutes
    sleep 900

    # Kill the ./run process using its PID
    killall -9 "$RUN_PID"

    # Start the ./collect script
    ./collect "$SUBFOLDER" 0 5
  done
done

