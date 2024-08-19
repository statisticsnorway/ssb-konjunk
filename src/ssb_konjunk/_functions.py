def input_valid_int() -> int:
    """Input function for valid int.

    Returns:
    -------
        valid_int (int): valid_int
    """
    # Get the desired year from the user
    while True:
        user_input = input("Input: ")

        try:
            valid_int = int(user_input)
            break  # Break the loop if a valid integer is entered
        except ValueError:
            print("Vennligst skriv inn et gyldig tall som f.eks.", 42)
    return valid_int
