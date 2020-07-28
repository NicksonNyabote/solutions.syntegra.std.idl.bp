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
import org.openbravo.model.ad.process.ProcessInstance;
import org.openbravo.model.ad.ui.Process;
import org.openbravo.module.idljava.proc.IdlServiceJava;

import au.com.bytecode.opencsv.CSVReader;

public class ESTUpload extends IdlServiceJava {

  public static boolean validateCell(String strCellPhone) {

    return (strCellPhone.matches("\\d{10}"));
  }

  public static boolean validatecharr(String strPrDefault) {

    // validate IsPrDefault
    return (strPrDefault.matches("[A-Z][a-zA-Z]*"));

  }

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

    Validator validator;
	
    final Connection con1 = OBDal.getInstance().getConnection();
    PreparedStatement st1 = con1.prepareStatement("delete from idlebp_esti_source");
    st1.executeUpdate();
	
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
    PreparedStatement st2 = con2.prepareStatement("select idlebp_finishesti()");
    st2.executeQuery();
    return true;
  }

  @Override
  public String getEntityName() {
    return "esti_source";
  }

  @Override
  public Parameter[] getParameters() {
    return new Parameter[] { new Parameter("GrowerNo", Parameter.STRING),
        new Parameter("GrowerName", Parameter.STRING), new Parameter("EstDate", Parameter.STRING),
        new Parameter("ProdCycle", Parameter.STRING), new Parameter("Scale", Parameter.STRING),
        new Parameter("Unit_Yield", Parameter.STRING),
        new Parameter("Gross_Yield", Parameter.STRING), new Parameter("Currency", Parameter.STRING),
        new Parameter("Price", Parameter.STRING), new Parameter("EstStatus", Parameter.STRING)

    };
  }

  @Override
  protected Object[] validateProcess(Validator validator, String... values) throws Exception {

    validator.checkNotNull(validator.checkString(values[0], 40, "GrowerNo"), "GrowerNo");
    validator.checkNotNull(validator.checkString(values[1], 60, "GrowerName"), "GrowerName");
    validator.checkNotNull(validator.checkString(values[3], 60, "ProdCycle"), "ProdCycle");
    return values;
  }

  @Override
  public BaseOBObject internalProcess(Object... values) throws Exception {
    return createEst((String) values[0], (String) values[1], (String) values[2], (String) values[3],
        (String) values[4], (String) values[5], (String) values[6], (String) values[7],
        (String) values[8], (String) values[9]);
  }

  public BaseOBObject createEst(final String strGrowerNo, final String strGrowerName,
      final String strEstDate, final String strProdCyc, final String loScale,
      final String loUnitYield, final String loGrossYield, final String strCurrency,
      final String loPrice, final String strStatus) throws Exception {

    esti_source estEx = null;

    if (estEx == null) {

      // Create New BP
      esti_source es = OBProvider.getInstance().get(esti_source.class);

      es.setREGNo(strGrowerNo);
      es.setEstimationDate(strEstDate);
      es.setProductionCycle(strProdCyc);
      es.setScale(loScale);
      es.setUnitYield(loUnitYield);
      es.setGrossYield(loGrossYield);
      es.setEstimatedPrice(loPrice);
      es.setEstimateStatus(strStatus);
      es.setTender(strCurrency);
      es.setGrowerName(strGrowerName);
      OBDal.getInstance().save(es);
      

      estEx = es;
    }
    return estEx;

  }

  public static boolean isValid(String s) {

    Pattern p = Pattern.compile("[7][0-9]{8}");

    Matcher m = p.matcher(s);
    return (m.find() && m.group().equals(s));
  }

  protected void ProcessES(esti_source es, boolean prev) throws Exception {

    completeES(es, prev);

    OBDal.getInstance().save(es);
    OBDal.getInstance().flush();

  }

  protected void completeES(esti_source es, boolean prev) throws Exception {

    OBContext.setAdminMode();

    final Process process = OBDal.getInstance().get(Process.class, "111");

    // Create the pInstance
    final ProcessInstance pInstance = OBProvider.getInstance().get(ProcessInstance.class);
    // sets its process
    pInstance.setProcess(process);
    // must be set to true
    pInstance.setActive(true);
    pInstance.setRecordID(es.getId());

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
            "INSERT INTO public.idlebp_esti_source(idlebp_esti_source_id, ad_org_id, ad_client_id, isactive, created, createdby, updated, updatedby, reg_no, grower_name, estimation_date, production_cycle, scale, unit_yield, gross_yield, tender, estimated_price, estimate_status, isvalid, comments) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

      } else {
        ps = connection.prepareStatement(
            "INSERT INTO public.idlebp_esti_source(idlebp_esti_source_id, ad_org_id, ad_client_id, isactive, created, createdby, updated, updatedby, reg_no, grower_name, estimation_date, production_cycle, scale, unit_yield, gross_yield, tender, estimated_price, estimate_status, isvalid, comments) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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