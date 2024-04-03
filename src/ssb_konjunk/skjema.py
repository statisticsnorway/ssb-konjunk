import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Callable
from datetime import datetime
import pandas as pd
from dapla import FileClient

fs = FileClient.get_gcs_file_system()


def get_xml_root(xml_file: str) -> ET.Element:
    """Funtion to get xml root on Dapla.
    
    Args:
        xml_file: Strin value for xml filepath.
    
    Returns:
        ET.Element: Root of xml file."""
    with fs.open(xml_file, mode="r") as f:
        single_xml = f.read()
    return ET.fromstring(single_xml)


def return_txt_xml(root: ET.Element, child: str):
    """
    Function to return text value from child element in xml file.

    Args:
        root: Root with all data stored in a branch like structure.
        child: String value to find child element which contains a value.

    Returns:
        str: Returns string value from child element.
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
    """Class object to handle metadata from a reportee."""

    # folder_path: str = None
    # xml_file: str = None
    raNummer: str = None
    """str: String containing a number to id the collection schema."""

    delregNr: str = None
    """str: String containing a number to id the data collection."""

    periodeFomDato: str = None
    """str: String for date of start of period for the data collection."""

    periodeTomDato: str = None
    """str: String for data of end of period for the data collection."""

    enhetsNavn: str = None
    """str: String for the name of the reportee."""

    enhetsType: str = None
    """str: String to describe if the reportee is a "foretak" or "virksomhet"."""

    enhetsOrgNr: str = None
    """str: String for org number for the reportee"""

    reporteeOrgNr: str = None
    """str: String for orb number who reported the data, can be different, since sometimes this is outsourced."""

    kontaktPersonNavn: str = None
    """str: String with name of person who reported the data."""

    kontaktPersonEpost: str = None
    """str: String with email of person who reported the data."""

    kontaktPersonTelefon: str = None
    """str: String with phone number of person who reported the data."""

    kontaktInfoKommentar: str = None
    """str: String containing optional comment from reportee."""

    def _get_metadata(self, root: ET.Element):
        """
        Function to set metadata from xml file into the variebels in the object folder.

        Args:
            root: Root of a xml file.
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
class Reportee:
    """Dataclass for a reportee singular schema from Altinn."""
    
    xml_file: str = None
    """str: String for xml filepath."""

    xml_root: ET.Element = None
    """ET.Element: XML root."""

    metadata: MetaData = None
    """MetaData: Object of type MetaData containing metadata."""

    data: dict = None
    """dict: Dict with keys for data vars and value containing data values."""

    historical_data: dict = None
    """dict: Dict with keys for pd.series name and values containing pd.series."""

    editert_status: bool = False
    """bool: Bool flag for wether data is validated."""

    editert_av: str = None
    """str: String for name of person editing data."""

    editert_nar: str = None
    """str: Timestamp for when data was edited."""
    
    ueditert_verdi: dict = None
    """dict: Dict with keys for data vars and values containing unedited data."""


    # def get_filename_and_root(self):
    #     """
    #     Function to get xml root.
    #     """
    #     # This function fills inn attachment, xml_file and pdf
    #     for file in FileClient.ls(self.folder_path):
    #         if file.endswith(".xml"):
    #             self.xml_file = file
    #             self.xml_root = get_xml_root(file)
                

    def get_root(self):
        """
        Function to get xml root into class.
        """
        # This function fills inn attachment, xml_file and pdf
        self.xml_root = get_xml_root(self.xml_file)


    def get_metadata(self):
        """
        Function to set metadata from xml file into the variebels in the object skjema.
        """
        self.metadata = MetaData()

        self.metadata._get_metadata(self.xml_root)


    def get_data(self, data_vars: dict, checkboxList: list = None, unique_code: bool = False):
        """
        Function to get data from xml file.
        """
        self.data = {}
        # Dict contains variable names in key.
        # And dtype in value.
        for key, val in data_vars.items():
            value = return_txt_xml(self.xml_root,key)
            if value is not None:
                # Setting dtype using val, value from dict.
                self.data[key] = val(value)
        # Flatten out checkbox vars from Altinn3
        if checkboxList is not None:
            for checkbox_var in checkboxList:
                self.data = _transform_dict_checkbox_var(self.data,checkbox_var,unique_code)
        
        
    def get_historical_data(self):
        """
        Function to query historical data from parquet folder based on metadata.
        """
        print("Ingen funksjon enda, ønsker å lese in listen med filmapper og query nødvendig data.")
                
    
    def editer(self, value_vars: dict = None, checks:List[Callable[[int], bool]] = None, manualEditVars: List[str] = None):
        """
        Function to check data. Controls data with functions from checks up against historical data.
        """
        # If we can find edit element in xml file, then we dont edit the file again.
        if return_txt_xml(self.xml_root,"editertData") is not None:
            self.editert_status = return_txt_xml(self.root,"editert_status")
            self.editert_av = return_txt_xml(self.root,"editert_av")
            self.editert_nar = return_txt_xml(self.root,"editert_nar")
            self.ueditert_verdi = return_txt_xml(self.root,"ueditert_verdi")
            return
        # Runs math checks on reportee value variables.
        if checks is not None:
            results = []
            for key, value in value_vars.items():
                for check in checks:
                    results.append(check(self.data[key],self.historical_data[value]))
            self.editert_av = "MASKINELT"
            self.editert_nar = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not any(results):
                self.editert_status = True
        # Sets edit status False if any of these variables are present.
        if manualEditVars is not None:
            found = any(element in self.data for element in manualEditVars)
            if found:
                self.editert_status = False


@dataclass
class Skjema:
    """Dataclass to represent a schema from Altinn."""

    folder_path: str = None
    """str: String to folder path for schema."""

    data_vars: dict = None
    """dict: Dict with keys for vars that contain data, and value representing datatype."""

    checkboxList: list = None
    """list: List with strings representing vars that are checkboxes in Altinn3."""

    unique_code: bool = False
    """bool: Bool flag to mark wether the shcema uses unique codes for dummies."""
    
    value_vars: dict = None
    """dict: Dict with string representing vars that are used for statistics."""

    checks: List[Callable[[int], bool]] = None
    """List[Callable[[int], bool]]: Callable list containing function to check data with."""
    
    manualEditVars: List[str] = None
    """List[str]: List with strings for variables that if present always makes machine data validation fail."""
    
    reportees: List[Reportee] = None
    """List[Reportee]: List with objects of type reportee. One for each unique reported schema from Altinn."""
    
    
    def get_reportees(self, year:int, month:int) -> List[Reportee]:
        """Function to get reportees for a period.
        
        Args:
            year: Int representing year.
            month: Int representing month.
        """
        print("Placeholder func")
        
    
    def set_reportee_data(self):
        """Function to run all functions for Reportee in reportees."""
        # Iterating over each unique xml file.
        for reportee in self.reportees:
            reportee.get_root()
            reportee.get_metadata()
            reportee.get_data(    
                checkboxList = self.checkboxList,
                unique_code = self.unique_code,
                data_vars = self.data_vars,
            )
            reportee.editer(
                value_vars = self.value_vars,
                checks = self.checks,
                manualEditVars = self.manualEditVars,
            )