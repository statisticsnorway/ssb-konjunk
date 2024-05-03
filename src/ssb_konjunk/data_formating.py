""" Funsjoner for å gjøre data formartering"""
import pandas as pd


def bytte_koder(df:pd.DataFrame,kodeliste:dict[str,str],kolonnenavn:str) -> pd.DataFrame:
    """Bytter koder.
    
    Funksjonen for å bytte kode i en kolonne. 
    
    
    Args:
        df: Pandas dataramme som vi skal sende inn.
        kodeliste: Ordbok med gammel og ny kode.
        kolonnenavn: Navn på kolonnen som skal byttes ut.
        
    Returns:
        Dataramme med ny kode.
        
    """
    
    for old, new in kodeliste.items():
        
        df.loc[df[kolonnenavn] == old, kolonnenavn] = str(new)
            
    return df    

