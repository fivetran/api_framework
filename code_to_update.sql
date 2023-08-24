select 
	a.AccountNumber,
	a.Account_Level__c,
	a.CreatedById,
	a.Champion_Relationship__c,
    o.IsDeleted
from elijah_db.salesforce.account a
		inner join elijah_db.salesforce.opportunity o on a.id = o.account_id
	limit 10;
