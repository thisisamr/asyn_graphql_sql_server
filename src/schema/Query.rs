use super::{
    Addresses::{get_all_addresses, Address, AddressQueryResponse, self},
    AspnetUsers::{self, get_all_users},
    Complaint::{get_all_complaints, ComplaintQueryResponse, self},
    ComplaintReply::{get_all_complaint_replys,  ComplaintReplyQueryResponse, self},
  
    PaymentTrasnsactions::{
        get_all_payment_transactions, PaymentTransaction, PaymentTransactionQueryResponse, self,
    },
    Request::{get_all_requests,getUpserts, QueryResponse, Request},
    RequestPriceDifference::{
        get_all_request_price_differences,
        RequestPriceDifferenceQueryResponse, self,
    },
    ShippingOrder::{get_all_shipping_orders, ShippingOrderQueryResponse, self},
    UserAddress::{self, get_all_user_addresses, UserAddressQueryResponse},
    UserProfile::{get_all_user_profiles, UserProfileQueryResponse, self},
};
use async_graphql::{Context, InputObject, Object, SimpleObject};

use sqlx::{Execute, Mssql, Pool, QueryBuilder};
#[derive(SimpleObject)]
pub struct Howdy {
    rows_affected: i32,
}
pub struct Query;
#[Object]
impl Query {
    async fn howdy<'ctx>(&self, ctx: &Context<'ctx>) -> Howdy {
        let pool = ctx.data::<Pool<Mssql>>().unwrap();
        let response = sqlx::query("SELECT TOP(1) id from Requests")
            .execute(pool)
            .await;
        match response {
            Ok(res) => Howdy {
                rows_affected: res.rows_affected() as i32,
            },
            Err(e) => {
                println!("{:?}", e);
                Howdy { rows_affected: 0 }
            }
        }
    }

    async fn requestsQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
        date: Option<String>,
        last_sync_timestamp: Option<String>,
    ) -> QueryResponse {
        get_all_requests(self, ctx, first, after, date, last_sync_timestamp).await
    }
    async fn updatedRequestsQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<Request>{
        getUpserts(self, ctx, last_sync_timestamp).await
    }
    async fn updatedAspNetUsersQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<AspnetUsers::AspnetUsers>{
        AspnetUsers::getUpserts(self, ctx, last_sync_timestamp).await
    }
    async fn usersQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<String>,
    ) -> AspnetUsers::UserQueryResponse {
        get_all_users(self, ctx, first, after).await
    }
    async fn addressesQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> AddressQueryResponse {
        get_all_addresses(self, ctx, first, after).await
    }
    async fn updatedAddressesQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<Address>{
        Addresses::getUpserts(self, ctx, last_sync_timestamp).await
    }

    async fn paymenttrasnsactionsQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> PaymentTransactionQueryResponse {
        get_all_payment_transactions(self, ctx, first, after).await
    }
    async fn updatedPaymentTransactionQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<PaymentTransaction>{
        PaymentTrasnsactions::getUpserts(self, ctx, last_sync_timestamp).await
    }
    async fn userprofilesQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> UserProfileQueryResponse {
        get_all_user_profiles(self, ctx, first, after).await
    }
    async fn updatedUserProfilesQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<UserProfile::UserProfile>{
        UserProfile::getUpserts(self, ctx, last_sync_timestamp).await
    }
    async fn shippingordersQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> ShippingOrderQueryResponse {
        get_all_shipping_orders(self, ctx, first, after).await
    }
    async fn updatedShippingOrdersQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<ShippingOrder::ShippingOrder>{
        ShippingOrder::getUpserts(self, ctx, last_sync_timestamp).await
    }
    async fn useraddressesQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> UserAddressQueryResponse {
        get_all_user_addresses(self, ctx, first, after).await
    }
   
    async fn updatedUserAddressesQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<UserAddress::UserAddress>{
        UserAddress::getUpserts(self, ctx, last_sync_timestamp).await
    }
    async fn requestpricedifferencesQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> RequestPriceDifferenceQueryResponse {
        get_all_request_price_differences(self, ctx, first, after).await
    }
    async fn updatedRequestPriceDifferenceQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<RequestPriceDifference::RequestPriceDifference>{
        RequestPriceDifference::getUpserts (self, ctx, last_sync_timestamp).await
    }
 //will be excluded froms sync
/*
    #[graphql(name = "PaymentTrasnsactionRequestPriceDifferenceQuery")]
    async fn PaymentTrasnsactionRequestPriceDifferenceQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> PaymentTransactionRequestPriceDifferenceQueryResponse {
        get_all_payment_transaction_request_price_difference(self, ctx, first, after).await
    }
  */
  
    async fn complaintsQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> ComplaintQueryResponse {
        get_all_complaints(self, ctx, first, after).await
    }
    async fn updatedComplaintsQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<Complaint::Complaint>{
        Complaint::getUpserts (self, ctx, last_sync_timestamp).await
    } 
   
    async fn complaintrepliesQuery<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
    ) -> ComplaintReplyQueryResponse {
        get_all_complaint_replys(self, ctx, first, after).await
    }
    async fn updatedComplaintRepliesQuery<'ctx>(&self,ctx: &Context<'ctx>,last_sync_timestamp: String)->Vec<ComplaintReply::ComplaintReply>{
        ComplaintReply::getUpserts (self, ctx, last_sync_timestamp).await
    } 
 

    async fn get_associated_records<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        #[graphql(name = "new_users")] new_users: Vec<AssociatedSelectors>,
    ) -> Associates {
        let pool = ctx.data::<Pool<Mssql>>().unwrap();
        let users_string ="select  Id as id,ArabicFullName as arabicfullname,Cast(AddedDate as varchar) as addeddate,FirstLogIn as firstlogin,Cast(ModifiedDate as varchar) as modifieddate,MakerId as makerid  ,Cast(Cast(DateOfBirth as date) as varchar ) as dateofbirth,AddressId as addressid,UserName as username, NormalizedUserName as normalizedusername,Email as email,NormalizedEmail as normalizedemail ,EmailConfirmed as emailconfirmed, Cast(PasswordHash as varchar) as passwordhash,CAST(SecurityStamp AS varchar) as securitystamp,CAST(ConcurrencyStamp AS varchar) as concurrencystamp,CAST(PhoneNumber AS varchar) as phonenumber,PhoneNumberConfirmed as phonenumberconfirmed , TwoFactorEnabled as twofactorenabled,LockoutEnabled as lockoutenabled,LockoutEnd as lockoutendl,AccessFailedCount as accessfailedcount,SyncStatus as sync_status  from AspNetUsers where Id in (";
        let users_query =
            query_builder_wrapper(users_string, &new_users, AssociatesEnum::ASPNETUSERS);
        let addresses_string ="select Id as id,Cast(Description as nvarchar) as description, DistrictId as districtid ,Cast(PropertyNumber As varchar) as property_number,FloorNumber as floor_number,ApartmentNumber as apartment_number,StreetName as streetname,UniqueMark as unique_mark,RequestId as requestid,CAST(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar)as modifieddate,Createdby as createdby,UpdatedBy as updatedby,RegionId as regionid, SyncStatus as sync_status,FloorNumberText as floornumbertext,EasternBorder as easternborder,EasternBorderLength as easternborderlength,MaritimeBorder as maritimeborder, MaritimeBorderLength as maritimeborderlength,TribalBorder as tribalborder,TribalBorderLength as tribalborderlength, WesternBorder as westernborder,WesternBorderLength as westernborderlength from Addresses where RequestId in (";
        let addresses_query =
            query_builder_wrapper(addresses_string, &new_users, AssociatesEnum::ADDRESSES);
        let userprofiles_string = "select Id as id,cast(TelephoneNumber as nvarchar) as telephonenumber, UserId as userid, Cast(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar) as modifieddate, cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,HasWhatsApp as haswhatsapp, PhoneNumberType as phonenumbertype, Cast(Description as nvarchar)as description,SyncStatus as sync_status   from UserProfiles where UserId in (";
        let userprofiles_query = query_builder_wrapper(
            userprofiles_string,
            &new_users,
            AssociatesEnum::USERPROFILES,
        );

        let aspnetusers: Vec<AspnetUsers::AspnetUsers> =
            sqlx::query_as(&users_query).fetch_all(pool).await.unwrap();

        let addresses: Vec<Address> = sqlx::query_as(&addresses_query)
            .fetch_all(pool)
            .await
            .unwrap();
        let userprofiles: Vec<UserProfile::UserProfile> = sqlx::query_as(&userprofiles_query)
            .fetch_all(pool)
            .await
            .unwrap();
        let mut useraddresses = Vec::new();
        if userprofiles.len() > 0 {
            let useraddresse_string = "select Id as  id, Cast(Description as nvarchar) as description,DistrictId as districtid,UserProfileId as userprofileid,Cast(AddedDate as varchar) as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar) as createdby ,Cast(UpdatedBy as nvarchar) as updatedby ,RegionId as regionid from UserAddresses where UserProfileId in (";
            let useraddresses_querey = query_builder_wrapper_customSelector(
                useraddresse_string,
                &userprofiles
                    .iter()
                    .map(|item| item.id.unwrap())
                    .collect::<Vec<i32>>(),
            );
            useraddresses = sqlx::query_as(&useraddresses_querey)
                .fetch_all(pool)
                .await
                .unwrap();
        }

        let paymenttrasnsactions_string="select Id as id,CAST(PaymentTime as varchar) as paymenttime,convert(nvarchar(36), MerchantRefNum) as merchantrefnum ,Price as price,PaymentAmount as paymentamount,FawryFees as fawryfees, PaymentMethod as paymentmethod,OrderStatus as orderstatus ,CAST(ReferenceNumber as nvarchar)as referencenumber,CAST(StatusCode as nvarchar) as statuscode,CAST(StatusDescription as nvarchar) as statusdescription,RequestId as requestid,Cast(AddedDate as varchar) as addeddate ,CAST(ModifiedDate as varchar) as modifieddate,cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,TransactionType as transactiontype,RefundedAmount as refundedamount, SyncStatus as sync_status,cast(UserId as nvarchar) as userid from PaymentTrasnsactions where OrderStatus=1 and RequestId in (";
        let paymenttrasnsactions_query = query_builder_wrapper(
            paymenttrasnsactions_string,
            &new_users,
            AssociatesEnum::PAYMENTTRANSACTIONS,
        );
        let paymenttrasnsactions = sqlx::query_as(&paymenttrasnsactions_query)
            .fetch_all(pool)
            .await
            .unwrap();
        let requestpricedifferences_string = "select Id as id,Price as price,RequestId as requestid,cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,Cast(UpdatedBy as nvarchar) as updatedby,OrderStatus as orderstatus,AreaDifference as areadifference,Cast(Description as nvarchar)as description,RequestAreaDifferenceStatus as requestareadifferencestatus, SubUnitAreaDifference as subunitareadifference from RequestPriceDifferences where RequestId in (";
        let requestpricedifferences_query = query_builder_wrapper(
            requestpricedifferences_string,
            &new_users,
            AssociatesEnum::REQUESTPRICEDIFFERENCE,
        );

        let requestpricedifferences = sqlx::query_as(&requestpricedifferences_query)
            .fetch_all(pool)
            .await
            .unwrap();
        let shippingorders_string = "select Id as id,RequestId as requestid,ShippingType as shippingtype,ShippingPrice as shippingprice, OfficeId as officeid,Longitude as longitude, Latitude as latitude,DistrictId as districtid, Cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,NumberOfCopies as numberofcopies,ApartmentNumber as apartmentnumber, cast( Description as nvarchar)as description, FloorNumber as floornumber, PropertyNumber as propertynumber,RegionId as regionid, cast(StreetName as nvarchar) as streetname,Cast(UniqueMark as nvarchar) as  uniquemark,ExtraCopiesPrice as extracopiesprice,OrderStatus as orderstatus,SyncStatus as sync_status,cast(EditCertificateInformation as nvarchar) as editcertificateinformation from ShippingOrders where RequestId in (";
        let shippingorders_query = query_builder_wrapper(
            shippingorders_string,
            &new_users,
            AssociatesEnum::SHIPPINGORDERS,
        );
        let shippingorders = sqlx::query_as(&shippingorders_query)
            .fetch_all(pool)
            .await
            .unwrap();
        let complaintswebsite_string = "select Id as id,Cast(Title as nvarchar) as title,Cast(Content as nvarchar) as content,ComplaintStatus as complaintstatus,ComplaintType as complainttype,Cast(MakerId as nvarchar) as  makerid,RequestId requestid,Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby,Cast(UserId as nvarchar)as userid from Complaints where RequestId in (";
        let complaintswebsite_query = query_builder_wrapper(
            complaintswebsite_string,
            &new_users,
            AssociatesEnum::COMPLAINTWEBSITE,
        );
        let complaintswebsite = sqlx::query_as(&complaintswebsite_query)
            .fetch_all(pool)
            .await
            .unwrap();
        let mut complaintreplies = Vec::new();
        if complaintswebsite.len() > 0 {
            let complaintreplies_string = "select Id as id,ComplaintId as complaintid,Cast(Content as nvarchar) as content,Cast( MakerId as nvarchar)as makerid, Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby from ComplaintReplies where ComplaintId in (";
            let complaintreplies_query = query_builder_wrapper_customSelector(
                complaintreplies_string,
                &complaintswebsite
                    .iter()
                    .map(|item: &Complaint::Complaint| item.id.unwrap())
                    .collect::<Vec<i32>>(),
            );
            complaintreplies = sqlx::query_as(&complaintreplies_query)
                .fetch_all(pool)
                .await
                .unwrap();
        };

        // println!("{:?}", q2);
        Associates {
            aspnetusers,
            addresses,
            userprofiles,
            useraddresses,
            paymenttrasnsactions,
            requestpricedifferences,
            shippingorders,
            complaintswebsite,
            complaintreplies,
        }
    }
}
pub enum AssociatesEnum {
    ASPNETUSERS,
    ADDRESSES,
    USERPROFILES,
    PAYMENTTRANSACTIONS,
    REQUESTPRICEDIFFERENCE,
    SHIPPINGORDERS,
    COMPLAINTWEBSITE,
}
fn query_builder_wrapper(
    columns_string: &str,
    selectors: &Vec<AssociatedSelectors>,
    tables: AssociatesEnum,
) -> String {
    let mut query_builder: QueryBuilder<Mssql> = QueryBuilder::new(columns_string);
    let mut separated = query_builder.separated(", ");
    match tables {
        AssociatesEnum::ASPNETUSERS => {
            for value_type in selectors.iter() {
                separated.push(format!("'{}'", value_type.userid.clone().unwrap()));
            }
        }
        AssociatesEnum::ADDRESSES => {
            for value_type in selectors.iter() {
                separated.push(format!("'{}'", value_type.id.unwrap()));
            }
        }
        AssociatesEnum::USERPROFILES => {
            for value_type in selectors.iter() {
                separated.push(format!("'{}'", value_type.userid.clone().unwrap()));
            }
        }
        AssociatesEnum::PAYMENTTRANSACTIONS => {
            for value_type in selectors.iter() {
                separated.push(format!("'{}'", value_type.id.clone().unwrap()));
            }
        }
        AssociatesEnum::REQUESTPRICEDIFFERENCE => {
            for value_type in selectors.iter() {
                separated.push(format!("{}", value_type.id.unwrap()));
            }
        }
        AssociatesEnum::SHIPPINGORDERS => {
            for value_type in selectors.iter() {
                separated.push(format!("{}", value_type.id.unwrap()));
            }
        }
        AssociatesEnum::COMPLAINTWEBSITE => {
            for value_type in selectors.iter() {
                separated.push(format!("{}", value_type.id.unwrap()));
            }
        }
    }
    separated.push_unseparated(") ");

    let query = query_builder.build();
    let q = query.sql().to_owned();
    q
}
fn query_builder_wrapper_customSelector(columns_string: &str, selectors: &Vec<i32>) -> String {
    let mut query_builder: QueryBuilder<Mssql> = QueryBuilder::new(columns_string);
    let mut separated = query_builder.separated(", ");
    for value_type in selectors.iter() {
        separated.push(format!("{}", value_type));
    }

    separated.push_unseparated(") ");

    let query = query_builder.build();
    let q = query.sql().to_owned();
    q
}
#[derive(InputObject, Debug)]
pub struct AssociatedSelectors {
    sqlserveraddedtime: Option<String>,
    requestnumber: Option<String>,
    //this shoudl be requestid
    id: Option<i32>,
    userid: Option<String>,
    #[graphql(name = "last_sync_timestamp")]
    last_sync_timestamp: Option<String>,
}
#[derive(SimpleObject, Debug)]
pub struct Associates {
    aspnetusers: Vec<AspnetUsers::AspnetUsers>,
    addresses: Vec<Address>,
    userprofiles: Vec<UserProfile::UserProfile>,
    useraddresses: Vec<UserAddress::UserAddress>,
    paymenttrasnsactions: Vec<PaymentTransaction>,
    requestpricedifferences: Vec<RequestPriceDifference::RequestPriceDifference>,
    shippingorders: Vec<ShippingOrder::ShippingOrder>,
    complaintswebsite: Vec<Complaint::Complaint>,
    complaintreplies: Vec<ComplaintReply::ComplaintReply>,
}
