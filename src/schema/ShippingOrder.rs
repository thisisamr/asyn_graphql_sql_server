use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct ShippingOrder {
    id: Option<i32>,
    requestid: Option<i32>,
    shippingtype: Option<i32>,
    shippingprice: Option<f64>,
    officeid: Option<i32>,
    longitude: Option<f64>,
    latitude: Option<f64>,
    districtid: Option<i32>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
    numberofcopies: Option<i32>,
    apartmentnumber: Option<i32>,
    description: Option<String>,
    floornumber: Option<i32>,
    propertynumber: Option<i32>,
    regionid: Option<i32>,
    streetname: Option<String>,
    uniquemark: Option<String>,
    extracopiesprice: Option<f64>,
    orderstatus: Option<i32>,
    #[graphql(name = "sync_status")]
    sync_status: Option<i32>,
    editcertificateinformation: Option<String>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct ShippingOrderEdge {
    cursor: Option<i32>,
    node: ShippingOrder,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct ShippingOrderQueryResponse {
    page_info: ShippingOrderPageInfo,
    edges: Vec<ShippingOrderEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct ShippingOrderPageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_shipping_orders<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> ShippingOrderQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) Id as id,RequestId as requestid,ShippingType as shippingtype,ShippingPrice as shippingprice, OfficeId as officeid,Longitude as longitude, Latitude as latitude,DistrictId as districtid, Cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,NumberOfCopies as numberofcopies,ApartmentNumber as apartmentnumber, cast( Description as nvarchar)as description, FloorNumber as floornumber, PropertyNumber as propertynumber,RegionId as regionid, cast(StreetName as nvarchar) as streetname,Cast(UniqueMark as nvarchar) as  uniquemark,ExtraCopiesPrice as extracopiesprice,OrderStatus as orderstatus,SyncStatus as sync_status,cast(EditCertificateInformation as nvarchar) as editcertificateinformation from ShippingOrders where Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) Id as id,RequestId as requestid,ShippingType as shippingtype,ShippingPrice as shippingprice, OfficeId as officeid,Longitude as longitude, Latitude as latitude,DistrictId as districtid, Cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,NumberOfCopies as numberofcopies,ApartmentNumber as apartmentnumber, cast( Description as nvarchar)as description, FloorNumber as floornumber, PropertyNumber as propertynumber,RegionId as regionid, cast(StreetName as nvarchar) as streetname,Cast(UniqueMark as nvarchar) as  uniquemark,ExtraCopiesPrice as extracopiesprice,OrderStatus as orderstatus,SyncStatus as sync_status,cast(EditCertificateInformation as nvarchar) as editcertificateinformation from ShippingOrders"#,
            first
        ),
    };

    let row: Vec<ShippingOrder> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id.unwrap();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from ShippingOrders where Id > @p1")
                .bind(&curs)
                .fetch_one(pool)
                .await;
        match secondquery {
            Ok(result) => {
                if result.count > 0 {
                    hasNext = true;
                }
            }
            Err(e) => println!("{:?}", e),
        }
        ShippingOrderQueryResponse {
            page_info: ShippingOrderPageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| ShippingOrderEdge {
                    cursor: Some(row.id.unwrap()),
                    node: row,
                })
                .collect(),
        }
    } else {
        ShippingOrderQueryResponse {
            page_info: ShippingOrderPageInfo {
                endCursor: 0,
                hasNextPage: false,
            },
            edges: Vec::new(),
        }
    }
}
pub async fn getUpserts<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    last_sync_timestamp: String,
) -> Vec<ShippingOrder> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as id,RequestId as requestid,ShippingType as shippingtype,ShippingPrice as shippingprice, OfficeId as officeid,Longitude as longitude, Latitude as latitude,DistrictId as districtid, Cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,NumberOfCopies as numberofcopies,ApartmentNumber as apartmentnumber, cast( Description as nvarchar)as description, FloorNumber as floornumber, PropertyNumber as propertynumber,RegionId as regionid, cast(StreetName as nvarchar) as streetname,Cast(UniqueMark as nvarchar) as  uniquemark,ExtraCopiesPrice as extracopiesprice,OrderStatus as orderstatus,SyncStatus as sync_status,cast(EditCertificateInformation as nvarchar) as editcertificateinformation from ShippingOrders where ModifiedDate >='{}'"#,
                parsed_value
            );
            // get all requests the
            sqlx::query_as(&q).fetch_all(pool).await.unwrap()
        }
        Err(e) => {
            println!("{:?}", e);
            Vec::new()
        }
    }
}