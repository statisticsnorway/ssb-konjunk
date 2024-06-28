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


def get_timestamp_monthly(*args) -> str|None:
    """Function to create timstamp if frequency is monthly"""
    
    if len(args) == 2:
        return f"p{args[0]}-{args[1]:02}"
    elif len(args) == 4:
        return f"p{args[0]}-{args[1]:02}_p{args[2]}-{args[3]:02}"
    else:
        noe_gikk_galt(*args)
        return None
    

def get_timestamp_yearly(*args) -> str|None:
    """Function to create timstamp if frequency is yearly"""
    
    if len(args) == 2:
        return f"p{args[0]}-p{args[1]}"
    else:
        noe_gikk_galt(*args)
        return None
    

def get_timestamp_quarter(*args) -> str|None:
    """Function to create timestamp if frequency is quarter"""
    
    if len(args) == 2:
        return f"p{args[0]}Q{args[1]}"
    elif len(args) == 4:
        return f"p{args[0]}Q{args[1]}_p{args[2]}Q{args[3]}"
    else:
        noe_gikk_galt(*args)
        return None
    
    
def get_timestamp_trimester(*args) -> str|None:
    """Function to create timestamp if frequency is trimester"""
    
    if len(args) == 2:
        return f"p{args[0]}T{args[1]}"
    elif len(args) == 4:
        return f"p{args[0]}T{args[1]}_p{args[2]}T{args[3]}"
    else:
        noe_gikk_galt(*args)
        return None
    

def get_timestamp_bimester(*args) -> str|None:
    """Function to create timestamp if frequency is bimester"""
    
    if len(args) == 2:
        return f"p{args[0]}B{args[1]}"
    elif len(args) == 4:
        return f"p{args[0]}B{args[1]}_p{args[2]}B{args[3]}"
    else:
        noe_gikk_galt(*args)
        return None
    
    
def get_timestamp_week(*args) -> str|None:
    """Function to create timstamp if frequency is week"""
    
    if len(args) == 2:
        return f"p{args[0]}-{args[1]:02}"
    elif len(args) == 4:
        return f"p{args[0]}W{args[1]:02}_p{args[2]}W{args[3]:02}"
    else:
        noe_gikk_galt(*args)
        return None

def get_ssb_timestamp(*args, frequency: str = "m") -> str | None:
    """Function to create a string in ssb timestamp format.

    Args:
        args: Up to six arguments with int, to create timestamp for.
        
    Returns:
        string|None: Returns time stamp in ssb format.
    """
    print(args)
    print(len(args))
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
    elif len(args) == 1:
        return f"p{args[0]}"
    else:
        valid_args = [arg for arg in args if arg]
        print(valid_args)
        print(len(valid_args))
        
        if frequency == "d":
            return get_timestamp_daily(*valid_args)
        
        if not check_even(valid_args) or len(valid_args)>4:
            print(f"For frekvens '{frequency}', må du ha enten to eller fire argumenter. Du har:",len(valid_args))
            return None
        else:
            if frequency == "m":
                return get_timestamp_monthly(*valid_args)
            if frequency == "y":
                return get_timestamp_yearly(*valid_args)
            if frequency == "q":
                return get_timestamp_quarter(*valid_args)
            if frequency == "t":
                return get_timestamp_trimester(*valid_args)
            if frequency == "b":
                return get_timestamp_bimester(*valid_args)
            if frequency == "w":
                return get_timestamp_week(*valid_args)