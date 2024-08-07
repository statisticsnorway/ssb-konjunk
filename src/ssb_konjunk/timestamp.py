"""Functions to create timestamp according to SSB standard."""


def check_even(number:int) -> bool:
    return len(number) % 2 == 0

### USIKKER PÅ OM JEG VIL BEHOLDE DENNE, DU KAN BRUKE f"{day:02}" FOR Å FÅ LEDENDE NULL.
def get_leading_zero(number:int) -> str:
    """Function to get leading zero on number for month and days"""
    return f"{number:02}"


def noe_gikk_galt(*args):
    print(f"Noe gikk galt! Ditt antall argumenter:{len(args)}, argumentene er:{args}")

    
def get_timestamp_daily(*args) -> str|None:
    """Function to create timestamp if frequency is daily"""
    
    if len(args) == 3:
        return f"p{args[0]}-{args[1]:02}-{args[2]:02}"
    elif len(args) == 6:
        return f"p{args[0]}-{args[1]:02}-{args[2]:02}_p{args[3]}-{args[4]:02}-{args[5]:02}"
    else:
        print("Ikke gyldig mende args, du har antall:",len(args))
        return none
    

def get_timestamp_yearly(*args) -> str|None:
    """Function to create timstamp if frequency is yearly"""
    
    if len(args) == 2:
        return f"p{args[0]}-p{args[1]}"
    else:
        noe_gikk_galt(*args)
        return None
    

def get_timestamp_special(*args, frequency:str) -> str|None:
    """Function to create timestamp if frequency is now Y or D."""
    
    if len(args) == 2:
        return f"p{args[0]}{frequency}{args[1]:02}"
    elif len(args) == 4:
        return f"p{args[0]}{frequency}{args[1]:02}_p{args[2]}{frequency}{args[3]:02}"
    else:
        noe_gikk_galt(*args)
        return None


def get_ssb_timestamp(*args, frequency: str = "M") -> str | None:
    """Function to create a string in ssb timestamp format.

    Args:
        args: Up to six arguments with int, to create timestamp for.
        
    Returns:
        string|None: Returns time stamp in ssb format.
    """
    if len(args) > 6:
        print(
            "Du kan ikke ha flere enn seks argumenter for å lage en ssb timestamp. Du har",
            len(args),
        )
        return None
    elif all(arg is None for arg in args):
        return None
    elif not args[0]:
        print("Mangler start år, timestamp blir da None. Vurder å fylle inn start år.")
        return None
    elif len(args) == 1 and frequency == "Y":
        return f"p{args[0]}"

    else:
        
        valid_args = [arg for arg in args if arg]
        
        if frequency == "D":
            return get_timestamp_daily(*valid_args)
        
        if not check_even(valid_args) or len(valid_args)>4:
            print(f"For frekvens '{frequency}', må du ha enten to eller fire argumenter. Du har:",len(valid_args))
            return None
        else:
            if frequency == "Y":
                return get_timestamp_yearly(*valid_args)
            else:
                if frequency == "M":
                    frequency = "-"
                return get_timestamp_special(*valid_args,frequency=frequency)