use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct Address {
    id: i32,
    description: Option<String>,
    districtid: Option<i32>,
    #[graphql(name = "property_number")]
    property_number: Option<String>,
    #[graphql(name = "floor_number")]
    floor_number: Option<i32>,
    #[graphql(name = "apartment_number")]
    apartment_number: Option<String>,
    streetname: Option<String>,
    #[graphql(name = "unique_mark")]
    unique_mark: Option<String>,
    requestid: Option<i32>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
    regionid: Option<i32>,
    #[graphql(name = "sync_status")]
    sync_status: Option<i32>,
    floornumbertext: Option<String>,
    easternborder: Option<String>,
    easternborderlength: Option<String>,
    maritimeborder: Option<String>,
    maritimeborderlength: Option<String>,
    tribalborder: Option<String>,
    tribalborderlength: Option<String>,
    westernborder: Option<String>,
    westernborderlength: Option<String>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct AddressEdge {
    cursor: Option<i32>,
    node: Address,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct AddressQueryResponse {
    page_info: AddressPageInfo,
    edges: Vec<AddressEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct AddressPageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_addresses<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> AddressQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) Id as id,Cast(Description as nvarchar(4000)) as description, DistrictId as districtid ,Cast(PropertyNumber As nvarchar) as property_number,FloorNumber as floor_number,ApartmentNumber as apartment_number,Cast(StreetName as nvarchar(4000)) as streetname, Cast(UniqueMark as nvarchar(4000)) as unique_mark,RequestId as requestid,CAST(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar) as createdby, Cast(UpdatedBy as nvarchar) as updatedby,RegionId as regionid, SyncStatus as sync_status,FloorNumberText as floornumbertext,EasternBorder as easternborder,EasternBorderLength as easternborderlength,MaritimeBorder as maritimeborder, MaritimeBorderLength as maritimeborderlength,TribalBorder as tribalborder,TribalBorderLength as tribalborderlength, WesternBorder as westernborder,WesternBorderLength as westernborderlength from Addresses  where Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) Id as id,Cast(Description as nvarchar(4000)) as description, DistrictId as districtid ,Cast(PropertyNumber As nvarchar) as property_number,FloorNumber as floor_number,ApartmentNumber as apartment_number,Cast(StreetName as nvarchar(4000)) as streetname, Cast(UniqueMark as nvarchar(4000)) as unique_mark,RequestId as requestid,CAST(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar) as createdby, Cast(UpdatedBy as nvarchar) as updatedby,RegionId as regionid, SyncStatus as sync_status,FloorNumberText as floornumbertext,EasternBorder as easternborder,EasternBorderLength as easternborderlength,MaritimeBorder as maritimeborder, MaritimeBorderLength as maritimeborderlength,TribalBorder as tribalborder,TribalBorderLength as tribalborderlength, WesternBorder as westernborder,WesternBorderLength as westernborderlength from Addresses"#,
            first
        ),
    };

    let row: Vec<Address> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id;
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from Addresses where Id > @p1")
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
        AddressQueryResponse {
            page_info: AddressPageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| AddressEdge {
                    cursor: Some(row.id.clone()),
                    node: row,
                })
                .collect(),
        }
    } else {
        AddressQueryResponse {
            page_info: AddressPageInfo {
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
) -> Vec<Address> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as id,Cast(Description as nvarchar(4000)) as description, DistrictId as districtid ,Cast(PropertyNumber As nvarchar) as property_number,FloorNumber as floor_number,ApartmentNumber as apartment_number,Cast(StreetName as nvarchar(4000)) as streetname, Cast(UniqueMark as nvarchar(4000)) as unique_mark,RequestId as requestid,CAST(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar) as createdby, Cast(UpdatedBy as nvarchar) as updatedby,RegionId as regionid, SyncStatus as sync_status,FloorNumberText as floornumbertext,EasternBorder as easternborder,EasternBorderLength as easternborderlength,MaritimeBorder as maritimeborder, MaritimeBorderLength as maritimeborderlength,TribalBorder as tribalborder,TribalBorderLength as tribalborderlength, WesternBorder as westernborder,WesternBorderLength as westernborderlength from Addresses  where ModifiedDate >='{}'"#,
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