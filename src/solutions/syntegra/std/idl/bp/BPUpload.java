package solutions.syntegra.std.idl.bp;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openbravo.base.exception.OBException;
import org.openbravo.base.provider.OBProvider;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.base.structure.BaseOBObject;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.reference.PInstanceProcessData;
import org.openbravo.erpCommon.utility.OBError;
import org.openbravo.erpCommon.utility.Utility;
import org.openbravo.idl.proc.Parameter;
import org.openbravo.idl.proc.Validator;
import org.openbravo.idl.proc.Value;
import org.openbravo.model.ad.domain.Reference;
import org.openbravo.model.ad.process.ProcessInstance;
import org.openbravo.model.ad.ui.Process;
import org.openbravo.model.common.businesspartner.Category;
import org.openbravo.model.financialmgmt.accounting.UserDimension1;
import org.openbravo.model.project.Project;
import org.openbravo.module.idljava.proc.IdlServiceJava;
import org.openbravo.service.web.ResourceNotFoundException;

import com.solutions.syntegra.producerregistration.data.synpreg_bank;
import com.solutions.syntegra.producerregistration.data.synpreg_bankbranch;
import com.solutions.syntegra.producerregistration.data.synpreg_group;
//import org.openbravo.model.ad.domain.List;
import com.solutions.syntegra.producerregistration.data.synpreg_prod_scheme;

import au.com.bytecode.opencsv.CSVReader;

public class BPUpload extends IdlServiceJava {

  

  private bpsource Previousbp;
  private int TotalLines;
  private int ActualLine;

  @Override
  protected boolean executeImport(String filename, boolean insert) throws Exception {

    CSVReader reader = new CSVReader(new FileReader(filename), ',', '\"', '\\', 0, false, true);

    List AllLinesList = reader.readAll();
    TotalLines = AllLinesList.size();
    Previousbp = null;

    String[] nextLine;

    // Check header
    nextLine = (String[]) AllLinesList.get(0);
    if (nextLine == null) {
      throw new OBException(Utility.messageBD(conn, "IDLJAVA_HEADER_MISSING", vars.getLanguage()));
    }
    Parameter[] parameters = getParameters();
    if (parameters.length > nextLine.length) {
      throw new OBException(
          Utility.messageBD(conn, "IDLJAVA_HEADER_BAD_LENGTH", vars.getLanguage()));
    }

    final Connection con1 = OBDal.getInstance().getConnection();
    PreparedStatement st = con1.prepareStatement("delete from idlebp_bp_source");
    st.executeUpdate();

    Validator validator;

    for (int i = 1; i < TotalLines; i++) {

      nextLine = (String[]) AllLinesList.get(i);
      ActualLine = i;

      if (nextLine.length > 1 || nextLine[0].length() > 0) {
        // It is not an empty line

        // Validate types
        if (parameters.length > nextLine.length) {
          throw new OBException(
              Utility.messageBD(conn, "IDLJAVA_LINE_BAD_LENGTH", vars.getLanguage()));
        }

        validator = getValidator(getEntityName());
        Object[] result = validateProcess(validator, nextLine);
        if ("0".equals(validator.getErrorCode())) {
          finishRecordProcess(result);
        } else {
          OBDal.getInstance().rollbackAndClose();
          // We need rollback here becouse the intention is load ALL or NOTHING
          logRecordError(validator.getErrorMessage(), result);
        }
		
		if (i % 1000 == 0) {
			OBDal.getInstance().flush();
			OBDal.getInstance().getSession().clear();
		}
      }
    }
	
	OBDal.getInstance().flush();

    final Connection con2 = OBDal.getInstance().getConnection();
    PreparedStatement st2 = con2.prepareStatement("select idlebp_finishbp()");
    st2.executeQuery();

    return true;
  }

  @Override
  public String getEntityName() {
    return "bpsource";
  }

  @Override
  public Parameter[] getParameters() {
    return new Parameter[] { new Parameter("reg_no", Parameter.STRING),
        new Parameter("new_reg_no", Parameter.STRING), new Parameter("issuer", Parameter.STRING),
        new Parameter("card_type", Parameter.STRING), new Parameter("reg_status", Parameter.STRING),
        new Parameter("reg_date", Parameter.STRING), new Parameter("reg_ref", Parameter.STRING),
        new Parameter("reg_expirey_date", Parameter.STRING),
        new Parameter("is_pr_default", Parameter.STRING),
        new Parameter("bp_type", Parameter.STRING), 
		new Parameter("bp_name", Parameter.STRING),
		new Parameter("bp_category", Parameter.STRING),
        new Parameter("bp_status", Parameter.STRING),
        new Parameter("bp_status_date", Parameter.STRING), new Parameter("notes", Parameter.STRING),
        new Parameter("bp_flag", Parameter.STRING),
        new Parameter("custom_field_1", Parameter.STRING),
        new Parameter("custom_field_2", Parameter.STRING),
        new Parameter("custom_field_3", Parameter.STRING),
        new Parameter("surname", Parameter.STRING), new Parameter("first_name", Parameter.STRING),
        new Parameter("nid", Parameter.STRING), new Parameter("date_of_birth", Parameter.STRING),
        new Parameter("gender", Parameter.STRING),
        new Parameter("is_primary_person", Parameter.STRING),
        new Parameter("production_cycle", Parameter.STRING),
        new Parameter("pc_status", Parameter.STRING),
        new Parameter("pc_status_date", Parameter.STRING),
        new Parameter("is_pc_default", Parameter.STRING),
        new Parameter("producer_model", Parameter.STRING),
        new Parameter("producer_type", Parameter.STRING),
        new Parameter("production_scheme", Parameter.STRING),
        new Parameter("community_group", Parameter.STRING),
        new Parameter("town", Parameter.STRING),
        new Parameter("address_description", Parameter.STRING),
        new Parameter("directions", Parameter.STRING),
        new Parameter("gps_location", Parameter.STRING),
        new Parameter("cell_phone", Parameter.STRING), new Parameter("land_line", Parameter.STRING),
        new Parameter("email", Parameter.STRING),
        new Parameter("trading_account", Parameter.STRING),
        new Parameter("ta_status", Parameter.STRING),
        new Parameter("ta_status_date", Parameter.STRING),
        new Parameter("is_ta_default", Parameter.STRING), new Parameter("bank", Parameter.STRING),
        new Parameter("branch", Parameter.STRING), new Parameter("branch_code", Parameter.STRING),
        new Parameter("bank_acc_name", Parameter.STRING),
        new Parameter("bank_acc_number", Parameter.STRING),
        new Parameter("bank_acc_curr", Parameter.STRING),
        new Parameter("is_ba_default", Parameter.STRING)

    };
  }

  @Override
  protected Object[] validateProcess(Validator validator, String... values) throws Exception {

    validator.checkNotNull(validator.checkString(values[0], 40, "reg_no"), "reg_no");

    // validator.checknew_reg_no(values[1]);

    // After Validate go to catch Default Value if exits
    // values = getDefaultValues(values);

    return values;
  }

  @Override
  public BaseOBObject internalProcess(Object... values) throws Exception {
    return createbpsource((String) values[0], (String) values[1], (String) values[2],
        (String) values[3], (String) values[4], (String) values[5], (String) values[6],
        (String) values[7], (String) values[8], (String) values[9], (String) values[10],
        (String) values[11], (String) values[12], (String) values[13], (String) values[14],
        (String) values[15], (String) values[16], (String) values[17], (String) values[18],
        (String) values[19], (String) values[20], (String) values[21], (String) values[22],
        (String) values[23], (String) values[24], (String) values[25], (String) values[26],
        (String) values[27], (String) values[28], (String) values[29], (String) values[30],
        (String) values[31], (String) values[32], (String) values[33], (String) values[34],
        (String) values[35], (String) values[36], (String) values[37], (String) values[38],
        (String) values[39], (String) values[40], (String) values[41], (String) values[42],
        (String) values[43], (String) values[44], (String) values[45], (String) values[46],
        (String) values[47], (String) values[48], (String) values[49], (String) values[50]);
  }

  public BaseOBObject createbpsource(final String strRegNo, final String strNewRegNo,
      final String strIssuer, final String strCardType, final String strRegStatus,
      final String strRegDate, final String strRegRef, final String strRegExpireyDate,
      final String strPrDefault, final String strType, final String strName, final String strCategory, final String strStatus,
      final String strStatusDate, final String strNotes, final String strFlag,
      final String strCustomField1, final String strCustomField2, final String strCustomField3,
      final String strSurname, final String strFirstName, final String strNid,
      final String strDateOfBirth, final String strGender, final String strPrimaryPerson,
      final String strProductionCycle, final String strPcStatus, final String strPcStatusDate,
      final String strIsPcDefault, final String strProducerModel, final String strProducerType,
      final String strProductionScheme,
      final String strCommunityGroup,
      final String strTown, final String strAddressDescription, final String strDirections,
      final String strGpsLocation, final String strCellPhone, final String strLandLine,
      final String strEmail, final String strTradingAccount, final String strTaStatus,
      final String strTaStatusDate, final String strIsTaDefault, final String strBank,
      final String strBranch, final String strBranchCode, final String strBankAccName,
      final String strBankAccNumber, final String strBankAccCurr, final String strIsBaDefault)
      throws Exception {


    bpsource bpsourceEx = null;

    if (bpsourceEx == null) {

      // Create New BP
      bpsource bp = OBProvider.getInstance().get(bpsource.class);

      bp.setREGNo(strRegNo);
      bp.setNEWRegNo(strNewRegNo);
      bp.setIssuer(strIssuer);
      bp.setCardType(strCardType);
      bp.setREGStatus(strRegStatus);
      bp.setREGDate(strRegDate);
      bp.setREGRef(strRegRef);
      bp.setREGExpireyDate(strRegExpireyDate);
	  bp.setPrDefault(strPrDefault);
	  bp.setType(strType);
      bp.setName(strName);
	  bp.setCategory(strCategory);
      bp.setStatus(strStatus);
      bp.setStatusDate(strStatusDate);
      bp.setNotes(strNotes);
      bp.setFlag(strFlag);
      bp.setCustomField1(strCustomField1);
      bp.setCustomField2(strCustomField2);
      bp.setCustomField3(strCustomField3);
      bp.setSurname(strSurname);
      bp.setFirstName(strFirstName);
      bp.setNid(strNid);
      bp.setDateOfBirth(strDateOfBirth);
      bp.setGender(strGender);
	  bp.setPrimaryPerson(strPrimaryPerson);
      bp.setProductionCycle(strProductionCycle);
      bp.setPCStatus(strPcStatus);
      bp.setPCStatusDate(strPcStatusDate);
	  bp.setPcDefault(strIsPcDefault);
      bp.setProducerModel(strProducerModel);
      bp.setProducerType(strProducerType);
	  bp.setProductionScheme(strProductionScheme);
	  bp.setCommunityGroup(strCommunityGroup);
      bp.setTown(strTown);
      bp.setAddressDescription(strAddressDescription);
      bp.setDirections(strDirections);
      bp.setGPSLocation(strGpsLocation);
      bp.setCellPhone(strCellPhone);
      bp.setLandLine(strLandLine);
      bp.setEmail(strEmail);
	  bp.setTradingAccount(strTradingAccount);
      bp.setTAStatus(strTaStatus);
      bp.setTAStatusDate(strTaStatusDate);
	  bp.setTaDefault(strIsTaDefault);
	  bp.setBank(strBank);
	  bp.setBranch(strBranch);
      bp.setBranchCode(strBranchCode);
      bp.setBankAccName(strBankAccName);
      bp.setBankAccNumber(strBankAccNumber);
      bp.setBankAccCurr(strBankAccCurr);
	  bp.setBaDefault(strIsBaDefault);
      OBDal.getInstance().save(bp);
      

      Previousbp = bp;
    }
    return Previousbp;
  }


  protected void ProcessBP(bpsource bp, boolean prev) throws Exception {

    completeBP(bp, prev);
    // invoice.setIDLEIPROCESS("A");

    OBDal.getInstance().save(bp);
    OBDal.getInstance().flush();

  }

  protected void completeBP(bpsource bp, boolean prev) throws Exception {

    OBContext.setAdminMode();

    // get the process, we know that 199 is the generate shipments from invoice sp
    final Process process = OBDal.getInstance().get(Process.class, "111");

    // Create the pInstance
    final ProcessInstance pInstance = OBProvider.getInstance().get(ProcessInstance.class);
    // sets its process
    pInstance.setProcess(process);
    // must be set to true
    pInstance.setActive(true);
    pInstance.setRecordID(bp.getId());

    // persist to the db
    OBDal.getInstance().save(pInstance);

    // flush, this gives pInstance an ID
    OBDal.getInstance().flush();

    // call the SP
    try {
      // first get a connection
      final Connection connection = OBDal.getInstance().getConnection();
      // connection.createStatement().execute("CALL M_InOut_Create0(?)");
      PreparedStatement ps = null;
      final Properties obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
      if (obProps.getProperty("bbdd.rdbms") != null
          && obProps.getProperty("bbdd.rdbms").equals("POSTGRE")) {
        ps = connection.prepareStatement(
            "INSERT INTO public.idlebp_bp_source(idlebp_bp_source_id, ad_org_id, ad_client_id, isactive, created, createdby, updated, updatedby, reg_no, new_reg_no, issuer, card_type, reg_status, reg_date, reg_ref, reg_expirey_date, is_pr_default, bp_type, bp_name,bp_category, bp_status, bp_status_date, notes, bp_flag, custom_field_1, custom_field_2, custom_field_3, surname, first_name, national_id, date_of_birth, gender, is_primary_person, production_cycle, pc_status, pc_status_date, is_pc_default, producer_model, producer_type, production_scheme, community_group, town, address_description, directions, gps_location, cell_phone, land_line, email, trading_account, ta_status, ta_status_date, is_ta_default, bank, branch, branch_code, bank_acc_name, bank_acc_number, bank_acc_curr, is_ba_default, isvalid, comments) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

      } else {
        ps = connection.prepareStatement(
            "INSERT INTO public.idlebp_bp_source(idlebp_bp_source_id, ad_org_id, ad_client_id, isactive, created, createdby, updated, updatedby, reg_no, new_reg_no, issuer, card_type, reg_status, reg_date, reg_ref, reg_expirey_date, is_pr_default, bp_type, bp_name,bp_category, bp_status, bp_status_date, notes, bp_flag, custom_field_1, custom_field_2, custom_field_3, surname, first_name, national_id, date_of_birth, gender, is_primary_person, production_cycle, pc_status, pc_status_date, is_pc_default, producer_model, producer_type, production_scheme, community_group, town, address_description, directions, gps_location, cell_phone, land_line, email, trading_account, ta_status, ta_status_date, is_ta_default, bank, branch, branch_code, bank_acc_name, bank_acc_number, bank_acc_curr, is_ba_default, isvalid, comments) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      }
      ps.setString(1, pInstance.getId());
      ps.execute();

    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    // refresh the pInstance as the SP has changed it
    OBDal.getInstance().getSession().refresh(pInstance);

    if (pInstance.getResult() == 0) {
      // Error Processing
      OBError myMessage = Utility.getProcessInstanceMessage(conn, vars,
          getPInstanceData(pInstance));
      throw new OBException(
          (prev ? Utility.messageBD(conn, "IDLEI_PREVIOUS_INVOICE", vars.getLanguage())
              : Utility.messageBD(conn, "IDLEI_INVOICE", vars.getLanguage()))

              + Utility.messageBD(conn, "IDLEI_PROCESS_ERROR_STATUS", vars.getLanguage())
              + " ERROR: " + myMessage.getMessage());
    }

    OBContext.restorePreviousMode();

  }

  protected PInstanceProcessData[] getPInstanceData(ProcessInstance pInstance) throws Exception {
    Vector<java.lang.Object> vector = new Vector<java.lang.Object>(0);
    PInstanceProcessData objectPInstanceProcessData = new PInstanceProcessData();
    objectPInstanceProcessData.result = pInstance.getResult().toString();
    objectPInstanceProcessData.errormsg = pInstance.getErrorMsg();
    objectPInstanceProcessData.pMsg = "";
    vector.addElement(objectPInstanceProcessData);
    PInstanceProcessData pinstanceData[] = new PInstanceProcessData[1];
    vector.copyInto(pinstanceData);
    return pinstanceData;

  }

}