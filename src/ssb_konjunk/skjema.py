import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Callable
from datetime import datetime
import pandas as pd
from dapla import FileClient

fs = FileClient.get_gcs_file_system()


def get_xml_root(xml_file):
    with fs.open(xml_file, mode="r") as f:
        single_xml = f.read()
    return ET.fromstring(single_xml)


def return_txt_xml(root, child):
    """
    Function to return value from certain child element in root of xml object.

    Parameters
    ----------
    root (xml object of type ET package): Root with all data stored in a branch like structure.
    child (str): String value to find child element which contains a value.

    returns
    -------
        returns(str): Returns string value from child element.
    """
    for element in root.iter(child):
        return element.text


def _split_string(input_string: str) -> list[str]:
    """Split a string into a list of strings using ',' as the separator.
    Args:
        input_string: The input string to be split.
    Returns:
        list: A list of split strings.
    """
    return input_string.split(",")


def _transform_dict_checkbox_var(
    dictionary: dict[str, str],
    old_key: str,
    unique_code: bool = False,
    new_value: str = "1",
) -> dict[str, str]:
    """transform_dict_code_vars.
    Transform a dictionary by removing a key and using its value as a new key with a new value.
    Args:
        dictionary: The dictionary to be transformed.
        old_key: The key to remove from the dictionary.
        unique_code: Bool for if you are using unique codes from Klass or not.
        new_value: Optional new value to add, default is 1 as str.
    Returns:
        dict: The transformed dictionary.
    """
    if old_key in dictionary:
        value = dictionary.pop(old_key, None)

        values = _split_string(value)  # type: ignore
        for value in values:
            if unique_code is False:
                dictionary[old_key + value] = new_value
            else:
                dictionary[value] = new_value

    return dictionary


@dataclass
class MetaData:
    """
    Class object to handle metadata from a reportee

    Variables
    ---------
    xml_file (str): String with path to xml file with metadata about reportee and the data collection process.
    delregNr (str): String containing a number to id the data collection.
    periodeFomDato (str): String for date of start of period for the data collection.
    periodeTomDato (str): String for data of end of period for the data collection.
    enhetsNavn (str): String for the name of the reportee.
    enhetsType (str): String to describe if the reportee is a "foretak" or "virksomhet".
    enhetsOrgNr (str): String for org number for the reportee
    reporteeOrgNr (str): String for orb number who reported the data, can be different, since sometimes this is outsourced.
    kontaktPersonNavn (str): String with name of person who reported the data.
    kontaktPersonEpost (str): String with email of person who reported the data.
    kontaktPersonTelefon (str): String with phone number of person who reported the data.
    kontaktInfoKommentar (str): String containing optional comment from reportee.

    """

    # folder_path: str = None
    # xml_file: str = None
    raNummer: str = None
    delregNr: str = None
    periodeFomDato: str = None
    periodeTomDato: str = None
    enhetsNavn: str = None
    enhetsType: str = None
    enhetsOrgNr: str = None
    reporteeOrgNr: str = None
    kontaktPersonNavn: str = None
    kontaktPersonEpost: str = None
    kontaktPersonTelefon: str = None
    kontaktInfoKommentar: str = None

    def _get_metadata(self, root):
        """
        Function to set metadata from xml file into the variebels in the object folder.

        Parameters
        ----------
        self (class object): Folder class object.

            variable from self
            ------------------
            xml_file (str): String with path to xml_file containing metadata about reportee.

        returns
        -------
            returns(class object): Returns self with filled in variabels.
        """
        if root is None:
            print(f"Mangler xml fil")
        else:
            self.raNummer = return_txt_xml(root, "raNummer")
            self.delregNr = return_txt_xml(root, "delregNr")
            self.periodeFomDato = return_txt_xml(root, "periodeFomDato")
            self.periodeTomDato = return_txt_xml(root, "periodeTomDato")
            self.enhetsNavn = return_txt_xml(root, "enhetsNavn")
            self.enhetsType = return_txt_xml(root, "enhetsType")
            self.enhetsOrgNr = return_txt_xml(root, "enhetsOrgNr")
            self.reporteeOrgNr = return_txt_xml(root, "reporteeOrgNr")
            self.kontaktPersonNavn = return_txt_xml(root, "kontaktPersonNavn")
            self.kontaktPersonEpost = return_txt_xml(root, "kontaktPersonEpost")
            self.kontaktPersonTelefon = return_txt_xml(root, "kontaktPersonTelefon")
            self.kontaktInfoKommentar = return_txt_xml(root, "kontaktInfoKommentar")


@dataclass
class Skjema:
    folder_path: str = None
    checkboxList: list = None
    unique_code: bool = False
    data_vars: list = None
    value_vars: dict = None
    xml_file: str = None
    xml_root: ET.Element = None
    metadata: MetaData = None
    data: dict = None
    historical_data: dict = None
    checks: List[Callable[[int], bool]] = None
    editert_status: bool = False
    editert_av: str = None
    editert_nar: str = None
    ueditert_verdi: dict = None

    def get_filename_and_root(self):
        """
        Function to get filename

        Parameters
        ----------
        self (class object): Folder class object.

            variable from self
            ------------------
            folder_path (str): String with path to folder containg files about and from one reportee
        """
        # This function fills inn attachment, xml_file and pdf
        for file in FileClient.ls(self.folder_path):
            if file.endswith(".xml"):
                self.xml_file = file
                self.xml_root = get_xml_root(file)

    def get_metadata(self):
        """
        Function to set metadata from xml file into the variebels in the object skjema.

        Parameters
        ----------
        self (class object): Skjema class object.
        """
        self.metadata = MetaData()

        self.metadata._get_metadata(self.xml_root)
        
    def get_data(self):
        self.data = {}
        for var in self.data_vars:
            value = return_txt_xml(self.xml_root,var)
            if value is not None:
                self.data[var] = value
            
        if self.checkboxList is not None:
            for checkbox_var in self.checkboxList:
                self.data = _transform_dict_checkbox_var(self.data,checkbox_var,self.unique_code)
        
        
    def get_historical_data(self):
        print("Ingen funksjon enda, ønsker å lese in listen med filmapper og query nødvendig data.")
                
    
    def editer(self):
        if return_txt_xml(self.xml_root,"editertData") is not None:
            self.editert_status = return_txt_xml(self.root,"editert_status")
            self.editert_av = return_txt_xml(self.root,"editert_av")
            self.editert_nar = return_txt_xml(self.root,"editert_nar")
            self.ueditert_verdi = return_txt_xml(self.root,"ueditert_verdi")
        else:
            results = []
            for key, value in self.value_vars.items():
                for check in self.checks:
                    results.append(check(float(self.data[key]),self.historical_data[value]))
            self.editert_av = "MASKINELT"
            self.editert_nar = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not any(results):
                self.editert_status = True
