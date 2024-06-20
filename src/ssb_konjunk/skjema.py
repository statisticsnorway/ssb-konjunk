import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Callable
from datetime import datetime
import pandas as pd
from dapla import FileClient
import dapla as dp
import pendulum

#Local imports
from . import xml_handling
from .xml_handling import return_txt_xml
fs = FileClient.get_gcs_file_system()


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
    
    check_nr: list
    """list: Id for checks that failed when editing data."""
    
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
        self.xml_root = xml_handling.read_xml(self.xml_file,fs)


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
        
        
    def get_historical_data(self, path_dict:dict)->dict[str,pd.Series]:
        """
        Function to query historical data from parquet folder based on metadata.
        """
        for key,value in path_dict.items():
            
            series = dp.read_pandas(value,filters=[("orgnrbed","=",self.metadata.enhetsOrgNr)])[key].reset_index(drop=True)
            # Tilater flere kontroll variable. Må kunne derfor fylt hist data om den er tom og kunne legge til.
            if self.historical_data is None:
                self.historical_data = {key:series}
            else:
                self.historical_data[key] = series
                
    
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
            check_number = []
            for key, value in value_vars.items():
                for check in checks:
                    result, nr = check(self.data[key],self.historical_data[value])
                    results.append(result)
                    check_number.append(nr)
            self.editert_av = "MASKINELT"
            self.editert_nar = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if not any(results):
                self.editert_status = True
            else:
                self.editert_status = False
                index_checks = [index for index, value in enumerate(results) if value]
                self.check_nr = [check_number[i] for i in index_checks]
                
        # Sets edit status False if any of these variables are present.
        if manualEditVars is not None:
            found = any(element in self.data for element in manualEditVars)
            if found:
                self.editert_status = False
                
                
    def flatten_reportee_data(self,value_vars:dict):
        """Function to flatten objects to a pandas df."""
        reportee_dict = {
            "skjema":self.metadata.raNummer,
            "delreg_nr":self.metadata.delregNr,
            "org_nr":self.metadata.reporteeOrgNr,
            "orgnrbed":self.metadata.enhetsOrgNr,
            "enhetsNavn":self.metadata.enhetsNavn,
            "kontaktPersonNavn":self.metadata.kontaktPersonNavn,
            "kontaktPersonEpost":self.metadata.kontaktPersonEpost,
            "kontaktPersonTelefon":self.metadata.kontaktPersonTelefon,
            "kontaktInfoKommentar":self.metadata.kontaktInfoKommentar,
            "periodeFomDato":self.metadata.periodeFomDato,
            "periodeTomDato":self.metadata.periodeTomDato,
            "editert_status":self.editert_status,
            "editert_av":self.editert_av,
            "editert_nar":self.editert_nar,
            "check_nr":self.check_nr,
        }
        
        for key,value in value_vars.items():
            reportee_dict[value] = self.data[key]
            
        return reportee_dict
        
        

@dataclass
class Skjema:
    """Dataclass to represent a schema from Altinn."""

    folder_xml_in: str = None
    """str: String to folder path for xml thats new."""
    
    folder_xml_out: str = None
    """str: String to folder path for xml files that are loaded."""
    
    folder_data: str = None
    """str: String to folder path where to store loaded data."""

    data_vars: dict = None
    """dict: Dict with keys for vars that contain data, and value representing datatype."""

    checkboxList: list = None
    """list: List with strings representing vars that are checkboxes in Altinn3."""

    unique_code: bool = False
    """bool: Bool flag to mark wether the shcema uses unique codes for dummies."""
    
    value_vars: dict = None
    """dict: Dict with string representing vars that are used for statistics."""
    
    historical_data_paths: dict = None
    """dict: Dict with keys for variables in the historical data and values for gcp paths."""

    checks: List[Callable[[int], bool]] = None
    """List[Callable[[int], bool]]: Callable list containing function to check data with."""
    
    manualEditVars: List[str] = None
    """List[str]: List with strings for variables that if present always makes machine data validation fail."""
    
    reportees: List[Reportee] = None
    """List[Reportee]: List with objects of type reportee. One for each unique reported schema from Altinn."""
    
    inndata: pd.DataFrame = None
    """Pandas dataframe with data ready to send to inndata."""
    
    
    def get_reportees(self):
        """Function to get reportees for a period.

        Args:
            path: Path in GCP.

        Returns:
            List[Reportee]: List with object of class Reportee.
        """

        list_reportees = []

        # MERK!!! HUSK AA FJERNE [:4] DET ER BARE MIDLERTIDIG FOR TEST!
        for file in fs.glob(f"{self.folder_xml_in}/*.xml")[:4]:
            print(file)
            list_reportees.append(Reportee(xml_file = file))

        self.reportees = list_reportees


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
            reportee.get_historical_data(path_dict=self.historical_data_paths)
            reportee.editer(
                value_vars = self.value_vars,
                checks = self.checks,
                manualEditVars = self.manualEditVars,
            )
            
        
    def create_inndata_file(self):
        """Function to take reportees and create a df for period."""
            
        # Convert objects to dictionaries
        dicts = [reporte.flatten_reportee_data(self.value_vars) for reporte in self.reportees]

        # Create DataFrame from dictionaries
        df = pd.DataFrame(dicts)

        self.inndata = df


    def send_inndata(self):
        """Function to send data to inndata."""
        if not self.reportees:
            print("Ingen nye skjemaer!")
            return
        
        df = self.inndata.copy()
        # Temporary filter Might change to assert error
        df = df.dropna(subset="org_nr")
        unique_date = df["periodeFomDato"].unique()
        
        for date in unique_date:
            
            df_date = df.loc[df["periodeFomDato"]==date].reset_index(drop=True)
            if len(df_date["periodeFomDato"].unique())==1 and len(df_date["periodeTomDato"].unique())==1:
                #Setting info for filename
                skjema = df_date.at[0,"skjema"].lower()
                periodFrom = df_date.at[0,"periodeFomDato"]
                periodTo = df_date.at[0,"periodeTomDato"]
                
                file_name = f"{skjema}_p{periodFrom}-p{periodTo}"
                
                if not fs.glob(f"{self.folder_data}/{file_name}*"):
                    print("Ingen fil lager fil")
                    dp.write_pandas(df_date,f"{self.folder_data}/{file_name}_v0.parquet")
                else:
                    print("fil finnes fra før, appenderer ny data!")
                    
                    df_old = dp.read_pandas(fs.glob(f"{self.folder_data}/{file_name}*")[0])
                    
                    df_new = pd.concat([df_old,df_date]).reset_index(drop=True)

                    dp.write_pandas(df_new,fs.glob(f"{self.folder_data}/{file_name}*")[0])
            else:
                assert print("Ulike periodelengder i datamateriallet!")
                #Sender lastede xml til lastet mappe.
        for reportee in self.reportees:
            path_out = f"{self.folder_xml_out}/{reportee.metadata.delregNr}"
            file_mv = reportee.xml_file.replace(self.folder_xml_in,path_out)
            fs.mv(reportee.xml_file,file_mv)