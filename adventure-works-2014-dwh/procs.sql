-- check if role exists
create or replace function public.role_exists(
    role_name varchar(100),
    can_login boolean
) returns boolean
language 'plpgsql'
as $BODY$
declare
    role_cursor cursor for select rolname
                           from pg_catalog.pg_roles as roles
                           where roles.rolname = role_name
                           and roles.rolcanlogin = can_login;
    role_info pg_catalog.pg_roles%ROWTYPE;
begin
    open role_cursor;
    fetch role_cursor into role_info;
    if (role_info.rolname is null) then
        return false;
    end if;
    close role_cursor;
    return true;
end;
$BODY$
;

-- application role
drop procedure if exists public.create_application_role;
create procedure public.create_application_role()
language 'plpgsql'
as $BODY$
declare
    role_exists boolean;
begin
    select into role_exists role_exists('application', false);
    if (role_exists = false) then
        create role application;
    end if;
end;$BODY$
;

-- DWH role
drop procedure if exists public.create_dwh_role;
create procedure public.create_dwh_role()
language 'plpgsql'
as $BODY$
declare
    role_exists boolean;
begin
    select into role_exists role_exists('dwh', true);
    if (role_exists = false) then
        create role dwh login password 'dwh';
        grant application to dwh;
    end if;
end;$BODY$
;

-- Converts the specified integer (which should be < 100 and > -1)
-- into a two character string, zero filling from the left
-- if the number is < 10.
create or replace function public.udf_two_digit_zero_fill(number_arg int) returns char(2)
language 'plpgsql'
as $BODY$
    declare
        result char(2);
    begin
--         if (number_arg > 9) then
--             result = to_char(number_arg, 'fm00');
--         else
--             result = to_char(number_arg, 'fm00');
--         end if;

        return to_char(number_arg, 'fm00');
    end;
$BODY$
;

-- Returns the smaller of two given timestamps
create or replace function public.udf_minimum_date(
    date1 timestamp,
    date2 timestamp
) returns timestamp
language plpgsql
as $BODY$
    declare
        date3 timestamp;
    begin
        if (date1 <= date2) then
            date3 = date1;
        else
            date3 = date2;
        end if;
        return date3;
    end;
$BODY$
;

-- Builds an ISO 8601 format date from a year, month, and day specified as integers.
create or replace function public.udf_build_ISO8601_date(
    year int,
    month int,
    day int
) returns timestamp
language plpgsql
as $BODY$
    begin
        return cast(to_char(year, '9999') || '-' || public.udf_two_digit_zero_fill(month)
            || '-' || public.udf_two_digit_zero_fill(day) || 'T00:00:00' as timestamp);
    end
$BODY$
;

create or replace procedure public.populate_dim_sales_territory(
    alternate_key integer,
    region character varying(100),
    country character varying(100),
    territory_group character varying(100),
    territory_image bytea
)
language plpgsql
as $BODY$
    declare
        territory_exists integer;
    begin
        select into territory_exists sales_territory_alternate_key
        from public.dim_sales_territory
        where sales_territory_alternate_key = alternate_key;
        if (territory_exists is null) then
            insert into public.dim_sales_territory (sales_territory_alternate_key,
                                                    sales_territory_region,
                                                    sales_territory_country,
                                                    sales_territory_group,
                                                    sales_territory_image) values (alternate_key,
                                                                                   region,
                                                                                   country,
                                                                                   territory_group,
                                                                                   territory_image);
        end if;
    end;
$BODY$
;