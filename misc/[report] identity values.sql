set nocount on;

select 
   [PC_CLINICIANETATRACKING max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_CLINICIANETATRACKING]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_CLINICIANETATRACKING]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_CLINICIANETATRACKING]'))
from [pcastage].[PC_CLINICIANETATRACKING];

select 
   [PC_ELECTIONADDENDUMREQUESTCONTACT max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_ELECTIONADDENDUMREQUESTCONTACT]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_ELECTIONADDENDUMREQUESTCONTACT]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_ELECTIONADDENDUMREQUESTCONTACT]'))
from [pcastage].[PC_ELECTIONADDENDUMREQUESTCONTACT];

select 
   [PC_ELECTIONADDENDUMREQUESTEDSTATUS max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_ELECTIONADDENDUMREQUESTEDSTATUS]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_ELECTIONADDENDUMREQUESTEDSTATUS]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_ELECTIONADDENDUMREQUESTEDSTATUS]'))
from [pcastage].[PC_ELECTIONADDENDUMREQUESTEDSTATUS];

select 
   [PC_ELECTIONADDENDUMREQUESTS max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_ELECTIONADDENDUMREQUESTS]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_ELECTIONADDENDUMREQUESTS]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_ELECTIONADDENDUMREQUESTS]'))
from [pcastage].[PC_ELECTIONADDENDUMREQUESTS];

select 
   [PC_PATIENTCONTACTINFOS max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_PATIENTCONTACTINFO]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_PATIENTCONTACTINFO]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_PATIENTCONTACTINFO]'))
from [pcastage].[PC_PATIENTCONTACTINFO];


select 
   [PC_PATIENTMEDICATIONSETUP max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_PATIENTMEDICATIONSETUP]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_PATIENTMEDICATIONSETUP]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_PATIENTMEDICATIONSETUP]'))
from [pcastage].[PC_PATIENTMEDICATIONSETUP];

select 
   [PC_POINTCARECONNECTIVITYLOGS max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_POINTCARECONNECTIVITYLOGS]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_POINTCARECONNECTIVITYLOGS]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_POINTCARECONNECTIVITYLOGS]'))
from [pcastage].[PC_POINTCARECONNECTIVITYLOGS];

select 
   [PC_POINTCAREIPV4LOGS max] = max(id) 
  ,[stage_ident_current] = IDENT_CURRENT('[pcastage].[PC_POINTCAREIPV4LOGS]')
  ,[dbo_ident_current] = IDENT_CURRENT('[dbo].[PC_POINTCAREIPV4LOGS]')
  ,[delta_mx+1] = (max(id)+1) - (IDENT_CURRENT('[dbo].[PC_POINTCAREIPV4LOGS]'))
from [pcastage].[PC_POINTCAREIPV4LOGS];


