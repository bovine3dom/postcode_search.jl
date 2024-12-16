# Postcode search generator

Makes an arrow hive partitioned table of postcode data.

Directory structure:

left_hand_bit_of_postcode / right_hand_bit_of_postcode [/ full record?]
left_hand_bit_of_postcode / directory_contents.arrow
directory_contents.arrow

directory_contents.arrow contains a list of all directories in that directory

If you use this to actually search through postcodes, make sure you comply with the OS/RM licence terms
