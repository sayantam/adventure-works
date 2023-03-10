-- roles
CALL public.create_application_role();
CALL public.create_dwh_role();

-- adventure_works_dw_build_version
drop table if exists public.adventure_works_dw_build_version;
create table public.adventure_works_dw_build_version(
    db_version   varchar(100),
    version_date timestamp
);

-- dim_account
drop table if exists public.dim_account cascade;
create table public.dim_account(
   account_key                          int generated by default as identity primary key,
   parent_account_key                   int references public.dim_account,
   account_code_alternate_key           int,
   parent_account_code_alternate_key    int,
   account_description                  varchar(100),
   account_type                         varchar(100),
   operator                             varchar(100),
   custom_members                       varchar(600),
   value_type                           varchar(100),
   custom_member_options                varchar(400)
);

-- dim_currency
drop table if exists public.dim_currency cascade;
create table public.dim_currency(
    currency_key            int generated by default as identity primary key,
    currency_alternate_key  char(6)           not null,
    currency_name           varchar(100)      not null
);

create unique index ak_dim_currency_currency_alternate_key
    on public.dim_currency(currency_alternate_key);

-- dim_date
drop table if exists public.dim_date cascade;
create table public.dim_date(
    date_key                    int         not null
        constraint pk_dim_date_date_key
            primary key,
    full_date_alternate_key     date        not null,
    day_number_of_week          int8        not null,
    english_day_name_of_week    varchar(20) not null,
    spanish_day_name_of_week    varchar(20) not null,
    french_day_name_of_week     varchar(20) not null,
    day_number_of_month         int8        not null,
    day_number_of_year          smallint    not null,
    week_number_of_year         int8        not null,
    english_month_name          varchar(20) not null,
    spanish_month_name          varchar(20) not null,
    french_month_name           varchar(20) not null,
    month_number_of_year        int8        not null,
    calendar_quarter            int8        not null,
    calendar_year               smallint    not null,
    calendar_semester           int8        not null,
    fiscal_quarter              int8        not null,
    fiscal_year                 smallint    not null,
    fiscal_semester             int8        not null
);

create unique index ak_dim_date_full_date_alternate_key
    on public.dim_date(full_date_alternate_key);

-- dim_department_group
drop table if exists public.dim_department_group cascade;
create table public.dim_department_group(
    department_group_key       int generated by default as identity
        constraint pk_dim_department_group
        primary key,
    parent_department_group_key int
        constraint fk_dim_department_group_dim_department_group
            references dim_department_group,
    department_group_name      varchar(100)
);

-- dim_organization
drop table if exists public.dim_organization cascade;
create table public.dim_organization(
    organization_key       int generated by default as identity
        constraint pk_dim_organization
        primary key,
    parent_organization_key int
        constraint fk_dim_organization_dim_organization
            references dim_organization,
    percentage_of_ownership varchar(32),
    organization_name      varchar(100),
    currency_key           int
        constraint fk_dim_organization_dim_currency
            references dim_currency
);

-- dim_product_category
drop table if exists public.dim_product_category cascade;
create table public.dim_product_category(
    product_category_key          int generated by default as identity
        constraint pk_dim_product_category_product_category_key
        primary key,
    product_category_alternate_key int
        constraint ak_dim_product_category_product_category_alternate_key
            unique,
    english_product_category_name  varchar(100) not null,
    spanish_product_category_name  varchar(100) not null,
    french_product_category_name   varchar(100) not null
);

drop table if exists public.dim_product_subcategory cascade;
create table public.dim_product_subcategory(
    product_subcategory_key          int generated by default as identity
        constraint pk_dim_product_subcategory_product_subcategory_key
        primary key,
    product_subcategory_alternate_key int
        constraint ak_dim_product_subcategory_product_subcategory_alternate_key
            unique,
    english_product_subcategory_name  varchar(100) not null,
    spanish_product_subcategory_name  varchar(100) not null,
    french_product_subcategory_name   varchar(100) not null,
    product_category_key             int
        constraint fk_dim_product_subcategory_dim_product_category
            references dim_product_category
);

-- dim_product
drop table if exists public.dim_product cascade;
create table public.dim_product(
    product_key            int generated by default as identity
        constraint pk_dim_product_product_key
        primary key,
    product_alternate_key   varchar(50),
    product_subcategory_key int
        constraint fk_dim_product_dim_product_subcategory
            references dim_product_subcategory,
    weight_unit_measure_code    char(6),
    size_unit_measure_code      char(6),
    english_product_name        varchar(100) not null,
    spanish_product_name        varchar(100) not null,
    french_product_name         varchar(100) not null,
    standard_cost               money,
    finished_goods_flag         bit          not null,
    color                       varchar(30) not null,
    safety_stock_level          smallint,
    reorder_point               smallint,
    list_price                  money,
    size                        varchar(100),
    size_range                  varchar(100),
    weight                      float,
    days_to_manufacture         int,
    product_line                char(4),
    dealer_price                money,
    class                       char(4),
    style                       char(4),
    model_name                  varchar(100),
    large_photo                 bytea,
    english_description         varchar(800),
    french_description          varchar(800),
    chinese_description         varchar(800),
    arabic_description          varchar(800),
    hebrew_description          varchar(800),
    thai_description            varchar(800),
    german_description          varchar(800),
    japanese_description        varchar(800),
    turkish_description         varchar(800),
    start_date                  timestamp,
    end_date                    timestamp,
    status                      varchar(14),
    constraint ak_dim_product_product_alternate_key_start_date
        unique (product_alternate_key, start_date)
);

-- dim_promotion
drop table if exists public.dim_promotion cascade;
create table public.dim_promotion (
    promotion_key             int generated by default as identity
        constraint pk_dim_promotion_promotion_key
            primary key,
    promotion_alternate_key    int
        constraint ak_dim_promotion_promotion_alternate_key
            unique,
    english_promotion_name     varchar(510),
    spanish_promotion_name     varchar(510),
    french_promotion_name      varchar(510),
    discount_pct              float,
    english_promotion_type     varchar(100),
    spanish_promotion_type     varchar(100),
    french_promotion_type      varchar(100),
    english_promotion_category varchar(100),
    spanish_promotion_category varchar(100),
    french_promotion_category  varchar(100),
    start_date                timestamp not null,
    end_date                  timestamp,
    min_qty                   int,
    max_qty                   int
);

-- dim_sales_reason
drop table if exists public.dim_sales_reason cascade;
create table public.dim_sales_reason(
    sales_reason_key          int generated by default as identity
        constraint pk_dim_sales_reason_sales_reason_key
            primary key,
    sales_reason_alternate_key int          not null,
    sales_reason_name         varchar(100) not null,
    sales_reason_reason_type   varchar(100) not null
);

-- dim_sales_territory
drop table if exists public.dim_sales_territory cascade;
create table public.dim_sales_territory(
    sales_territory_key          int generated by default as identity
        constraint pk_dim_sales_territory_sales_territory_key
            primary key,
    sales_territory_alternate_key int
        constraint ak_dim_sales_territory_sales_territory_alternate_key
            unique,
    sales_territory_region       varchar(100) not null,
    sales_territory_country      varchar(100) not null,
    sales_territory_group        varchar(100),
    sales_territory_image        bytea
);

-- dim_employee
drop table if exists public.dim_employee cascade;
create table public.dim_employee(
    employee_key                          int generated by default as identity
        constraint pk_dim_employee_employee_key
            primary key,
    parent_employee_key                    int
        constraint fk_dim_employee_dim_employee
            references dim_employee,
    employee_national_id_alternate_key       varchar(30),
    parent_employee_national_id_alternate_key varchar(30),
    sales_territory_key                    int
        constraint fk_dim_employee_dim_sales_territory
            references dim_sales_territory,
    first_name                            varchar(100) not null,
    last_name                             varchar(100) not null,
    middle_name                           varchar(100),
    name_style                            bit          not null,
    title                                varchar(100),
    hire_date                             date,
    birth_date                            date,
    login_id                              varchar(512),
    email_address                         varchar(100),
    phone                                varchar(50),
    marital_status                        nchar,
    emergency_contact_name                 varchar(100),
    emergency_contact_phone                varchar(50),
    salaried_flag                         bit,
    gender                               nchar,
    pay_frequency                         int8,
    base_rate                             money,
    vacation_hours                        smallint,
    sick_leave_hours                       smallint,
    current_flag                          bit          not null,
    sales_person_flag                      bit          not null,
    department_name                       varchar(100),
    start_date                            date,
    end_date                              date,
    status                               varchar(100),
    employee_photo                        bytea
);

-- dim_geography
drop table if exists public.dim_geography cascade;
create table public.dim_geography(
    geography_key             int generated by default as identity
        constraint pk_dim_geography_geography_key
            primary key,
    city                     varchar(60),
    state_province_code        varchar(6),
    state_province_name        varchar(100),
    country_region_code        varchar(6),
    english_country_region_name varchar(100),
    spanish_country_region_name varchar(100),
    french_country_region_name  varchar(100),
    postal_code               varchar(30),
    sales_territory_key        int
        constraint fk_dim_geography_dim_sales_territory
            references dim_sales_territory,
    ip_address_locator         varchar(30)
);

-- dim_customer
drop table if exists public.dim_customer cascade;
create table public.dim_customer (
    customer_key          int generated by default as identity
        constraint pk_dim_customer_customer_key
            primary key,
    geography_key         int
        constraint fk_dim_customer_dim_geography
            references dim_geography,
    customer_alternate_key varchar(30) not null,
    title                varchar(16),
    first_name            varchar(100),
    middle_name           varchar(100),
    last_name             varchar(100),
    name_style            bit,
    birth_date            date,
    marital_status        nchar,
    suffix               varchar(20),
    gender               varchar(2),
    email_address         varchar(100),
    yearly_income         money,
    total_children        int8,
    number_children_at_home int8,
    english_education     varchar(80),
    spanish_education     varchar(80),
    french_education      varchar(80),
    english_occupation    varchar(200),
    spanish_occupation    varchar(200),
    french_occupation     varchar(200),
    house_owner_flag       nchar,
    number_cars_owned      int8,
    address_line1         varchar(240),
    address_line2         varchar(240),
    phone                varchar(40),
    date_first_purchase    date,
    commute_distance      varchar(30)
);

create unique index ix_dim_customer_customer_alternate_key
    on dim_customer (customer_alternate_key);

-- dim_reseller
drop table if exists public.dim_reseller cascade;
create table public.dim_reseller (
    reseller_key          int generated by default as identity
        constraint pk_dim_reseller_reseller_key
            primary key,
    geography_key         int
        constraint fk_dim_reseller_dim_geography
            references dim_geography,
    reseller_alternate_key varchar(30)
        constraint ak_dim_reseller_reseller_alternate_key
            unique,
    phone                varchar(50),
    business_type         varchar(20)  not null,
    reseller_name         varchar(100) not null,
    number_employees      int,
    order_frequency       char,
    order_month           int8,
    first_order_year       int,
    last_order_year        int,
    product_line          varchar(100),
    address_line1         varchar(120),
    address_line2         varchar(120),
    annual_sales          money,
    bank_name             varchar(100),
    min_payment_type       int8,
    min_payment_amount     money,
    annual_revenue        money,
    year_opened           int
);

-- dim_scenario
drop table if exists public.dim_scenario cascade;
create table public.dim_scenario (
    scenario_key  int generated by default as identity
        constraint pk_dim_scenario
            primary key,
    scenario_name varchar(100)
);

-- fact_additional_international_product_description
drop table if exists public.fact_additional_international_product_description cascade;
create table public.fact_additional_international_product_description (
    product_key         int           not null,
    culture_name        varchar(100)  not null,
    product_description text not null,
    constraint pk_fact_additional_international_product_description_product_key_culture_name
        primary key (product_key, culture_name)
);

-- fact_call_center
drop table if exists public.fact_call_center;
create table public.fact_call_center (
    fact_call_center_id    int generated by default as identity
        constraint pk_fact_call_center_fact_call_center_id
            primary key,
    date_key             int          not null
        constraint fk_fact_call_center_dim_date
            references dim_date,
    wage_type            varchar(30) not null,
    shift               varchar(40) not null,
    level_one_operators   smallint     not null,
    level_two_operators   smallint     not null,
    total_operators      smallint     not null,
    calls               int          not null,
    automatic_responses  int          not null,
    orders              int          not null,
    issues_raised        smallint     not null,
    average_time_per_issue smallint     not null,
    service_grade        float        not null,
    date                timestamp,
    constraint ak_fact_call_center_date_key_shift
        unique (date_key, shift)
);

-- fact_currency_rate
drop table if exists public.fact_currency_rate;
create table public.fact_currency_rate (
    currency_key  int   not null
        constraint fk_fact_currency_rate_dim_currency
            references dim_currency,
    date_key      int   not null
        constraint fk_fact_currency_rate_dim_date
            references dim_date,
    average_rate  float not null,
    end_of_day_rate float not null,
    date         timestamp,
    constraint pk_fact_currency_rate_currency_key_date_key
        primary key (currency_key, date_key)
);

-- fact_finance
drop table if exists public.fact_finance;
create table public.fact_finance (
    finance_key         int generated by default as identity,
    date_key            int   not null
        constraint fk_fact_finance_dim_date
            references dim_date,
    organization_key    int   not null
        constraint fk_fact_finance_dim_organization
            references dim_organization,
    department_group_key int   not null
        constraint fk_fact_finance_dim_department_group
            references dim_department_group,
    scenario_key        int   not null
        constraint fk_fact_finance_dim_scenario
            references dim_scenario,
    account_key         int   not null
        constraint fk_fact_finance_dim_account
            references dim_account,
    amount             float not null,
    date               timestamp
);

-- fact_internet_sales
drop table if exists public.fact_internet_sales;
create table public.fact_internet_sales (
    product_key            int          not null
        constraint fk_fact_internet_sales_dim_product
            references dim_product,
    order_date_key          int          not null
        constraint fk_fact_internet_sales_dim_date
            references dim_date,
    due_date_key            int          not null
        constraint fk_fact_internet_sales_dim_date1
            references dim_date,
    ship_date_key           int          not null
        constraint fk_fact_internet_sales_dim_date2
            references dim_date,
    customer_key           int          not null
        constraint fk_fact_internet_sales_dim_customer
            references dim_customer,
    promotion_key          int          not null
        constraint fk_fact_internet_sales_dim_promotion
            references dim_promotion,
    currency_key           int          not null
        constraint fk_fact_internet_sales_dim_currency
            references dim_currency,
    sales_territory_key     int          not null
        constraint fk_fact_internet_sales_dim_sales_territory
            references dim_sales_territory,
    sales_order_number      varchar(40) not null,
    sales_order_line_number  int8      not null,
    revision_number        int8      not null,
    order_quantity         smallint     not null,
    unit_price             money        not null,
    extended_amount        money        not null,
    unit_price_discount_pct  float        not null,
    discount_amount        float        not null,
    product_standard_cost   money        not null,
    total_product_cost      money        not null,
    sales_amount           money        not null,
    tax_amt                money        not null,
    freight               money        not null,
    carrier_tracking_number varchar(50),
    customer_pon_umber      varchar(50),
    order_date             timestamp,
    due_date               timestamp,
    ship_date              timestamp,
    constraint pk_fact_internet_sales_sales_order_number_sales_order_line_number
        primary key (sales_order_number, sales_order_line_number)
);

-- fact_internet_sales_reason
drop table if exists public.fact_internet_sales_reason;
create table public.fact_internet_sales_reason (
    sales_order_number     varchar(40) not null,
    sales_order_line_number int8      not null,
    sales_reason_key       int          not null
        constraint fk_fact_internet_sales_reason_dim_sales_reason
            references dim_sales_reason,
    constraint pk_fact_internet_sales_reason_sales_order_number_sales_order_line_number_sales_reason_key
        primary key (sales_order_number, sales_order_line_number, sales_reason_key),
    constraint fk_fact_internet_sales_reason_fact_internet_sales
        foreign key (sales_order_number, sales_order_line_number) references fact_internet_sales
);

-- fact_product_inventory
drop table if exists public.fact_product_inventory;
create table public.fact_product_inventory (
    product_key   int   not null
        constraint fk_fact_product_inventory_dim_product
            references dim_product,
    date_key      int   not null
        constraint fk_fact_product_inventory_dim_date
            references dim_date,
    movement_date date  not null,
    unit_cost     money not null,
    units_in      int   not null,
    units_out     int   not null,
    units_balance int   not null,
    constraint pk_fact_product_inventory
        primary key (product_key, date_key)
);

-- fact_reseller_sales
drop table if exists public.fact_reseller_sales;
create table public.fact_reseller_sales (
    product_key            int          not null
        constraint fk_fact_reseller_sales_dim_product
            references dim_product,
    order_date_key          int          not null
        constraint fk_fact_reseller_sales_dim_date
            references dim_date,
    due_date_key            int          not null
        constraint fk_fact_reseller_sales_dim_date1
            references dim_date,
    ship_date_key           int          not null
        constraint fk_fact_reseller_sales_dim_date2
            references dim_date,
    reseller_key           int          not null
        constraint fk_fact_reseller_sales_dim_reseller
            references dim_reseller,
    employee_key           int          not null
        constraint fk_fact_reseller_sales_dim_employee
            references dim_employee,
    promotion_key          int          not null
        constraint fk_fact_reseller_sales_dim_promotion
            references dim_promotion,
    currency_key           int          not null
        constraint fk_fact_reseller_sales_dim_currency
            references dim_currency,
    sales_territory_key     int          not null
        constraint fk_fact_reseller_sales_dim_sales_territory
            references dim_sales_territory,
    sales_order_number      varchar(40) not null,
    sales_order_line_number  int8      not null,
    revision_number        int8,
    order_quantity         smallint,
    unit_price             money,
    extended_amount        money,
    unit_price_discount_pct  float,
    discount_amount        float,
    product_standard_cost   money,
    total_product_cost      money,
    sales_amount           money,
    tax_amt                money,
    freight               money,
    carrier_tracking_number varchar(50),
    customer_ponumber      varchar(50),
    order_date             timestamp,
    due_date               timestamp,
    ship_date              timestamp,
    constraint pk_fact_reseller_sales_sales_order_number_sales_order_line_number
        primary key (sales_order_number, sales_order_line_number)
);

-- fact_sales_quota
drop table if exists public.fact_sales_quota;
create table public.fact_sales_quota (
    sales_quota_key    int generated by default as identity
        constraint pk_fact_sales_quota_sales_quota_key
            primary key,
    employee_key      int      not null
        constraint fk_fact_sales_quota_dim_employee
            references dim_employee,
    date_key          int      not null
        constraint fk_fact_sales_quota_dim_date
            references dim_date,
    calendar_year     smallint not null,
    calendar_quarter  int8  not null,
    sales_amount_quota money    not null,
    date             timestamp
);

-- fact_survey_response
drop table if exists public.fact_survey_response;
create table public.fact_survey_response (
    survey_response_key             int generated by default as identity
        constraint pk_fact_survey_response_survey_response_key
            primary key,
    date_key                       int          not null
        constraint fk_fact_survey_response_date_key
            references dim_date,
    customer_key                   int          not null
        constraint fk_fact_survey_response_customer_key
            references dim_customer,
    product_category_key            int          not null,
    english_product_category_name    varchar(100) not null,
    product_subcategory_key         int          not null,
    english_product_subcategory_name varchar(100) not null,
    date                          timestamp
);

-- new_fact_currency_rate
drop table if exists public.new_fact_currency_rate;
create table public.new_fact_currency_rate (
    average_rate  real,
    currency_id   varchar(6),
    currency_date date,
    end_of_day_rate real,
    currency_key  int,
    date_key      int
);

-- prospective_buyer
drop table if exists public.prospective_buyer;
create table public.prospective_buyer (
    prospective_buyer_key  int generated by default as identity
        constraint pk_prospective_buyer_prospective_buyer_key
            primary key,
    prospect_alternate_key varchar(30),
    first_name            varchar(100),
    middle_name           varchar(100),
    last_name             varchar(100),
    birth_date            timestamp,
    marital_status        nchar,
    gender               varchar(2),
    email_address         varchar(100),
    yearly_income         money,
    total_children        int8,
    number_children_at_home int8,
    education            varchar(80),
    occupation           varchar(200),
    house_owner_flag       nchar,
    number_cars_owned      int8,
    address_line1         varchar(240),
    address_line2         varchar(240),
    city                 varchar(60),
    state_province_code    varchar(6),
    postal_code           varchar(30),
    phone                varchar(40),
    salutation           varchar(16),
    unknown              int
);

-- Views
create or replace view public.v_dm_prep
as
    select pc.english_product_category_name,
       coalesce(p.model_name, p.english_product_name) as model,
       c.customer_key,
       s.sales_territory_group as region,
       case
           when extract(month from current_timestamp) < extract(month from c.birth_date)
               then extract(year from c.birth_date - current_timestamp) - 1
           when extract(month from current_timestamp) = extract(month from c.birth_date)
               and extract(day from current_timestamp) < extract(day from c.birth_date)
               then extract(year from c.birth_date - current_timestamp) - 1
           else extract(year from c.birth_date - current_timestamp)
           end as age,
       case
           when c.yearly_income::numeric < 40000 then 'Low'
           when c.yearly_income::numeric > 60000 then 'High'
           else 'Moderate'
       end as income_group,
       d.calendar_year,
       d.fiscal_year,
       d.month_number_of_year as month,
       f.sales_order_number as order_number,
       f.sales_order_line_number as line_number,
       f.order_quantity as quantity,
       f.extended_amount as amount
    from public.fact_internet_sales f
    inner join public.dim_date d on d.date_key = f.order_date_key
    inner join public.dim_product p on p.product_key = f.product_key
    inner join public.dim_product_subcategory psc on p.product_subcategory_key = psc.product_subcategory_key
    inner join public.dim_product_category pc on psc.product_category_key = pc.product_category_key
    inner join public.dim_customer c on c.customer_key = f.customer_key
    inner join public.dim_geography g on c.geography_key = g.geography_key
    inner join public.dim_sales_territory s on g.sales_territory_key = s.sales_territory_key;

-- vTargetMail supports targeted mailing data model
-- Uses vDMPrep to determine if a customer buys a bike and joins to DimCustomer
create or replace view public.v_target_mail
as
select
    c.customer_key,
    c.geography_key,
    c.customer_alternate_key,
    c.title,
    c.first_name,
    c.middle_name,
    c.last_name,
    c.name_style,
    c.birth_date,
    c.marital_status,
    c.suffix,
    c.gender,
    c.email_address,
    c.yearly_income,
    c.total_children,
    c.number_children_at_home,
    c.english_education,
    c.spanish_education,
    c.french_education,
    c.english_occupation,
    c.spanish_occupation,
    c.french_occupation,
    c.house_owner_flag,
    c.number_cars_owned,
    c.address_line1,
    c.address_line2,
    c.phone,
    c.date_first_purchase,
    c.commute_distance,
    x.region,
    x.age,
    case x.bikes
        when 0 then 0
        else 1
        end as bike_buyer
from public.dim_customer c inner join (
    select
        customer_key,
        region,
        age,
        sum(
                case english_product_category_name
                    when 'Bikes' then 1
                    else 0
                    end
            ) as bikes
    from public.v_dm_prep
    group by
        customer_key,
        region,
        age
) as x on c.customer_key = x.customer_key;

-- vTimeSeries view supports the creation of time series data mining models.
--      - Replaces earlier bike models with successor models.
--      - Abbreviates model names to improve readability in mining model viewer
--      - Concatenates model and region so that table only has one input.
--      - Creates a date field indexed to monthly reporting date for use in prediction.
create or replace view public.v_time_series
as
    select
        case model
            when 'Mountain-100' then 'M200'
            when 'Road-150' then 'R250'
            when 'Road-650' then 'R750'
            when 'Touring-1000' then 'T1000'
            else left(model, 1) || right(model, 3)
        end || ' ' || region as model_region,
        (calendar_year::integer * 100) + "month"::integer as time_index,
        sum(quantity) as quantity,
        sum(amount) as amount,
        calendar_year,
        "month",
        public.udf_build_ISO8601_date(calendar_year::integer, "month"::integer, 25) as reporting_date
    from
        public.v_dm_prep
    where
        model in ('Mountain-100', 'Mountain-200', 'Road-150', 'Road-250',
            'Road-650', 'Road-750', 'Touring-1000')
    group by
        case model
            when 'Mountain-100' then 'M200'
            when 'Road-150' then 'R250'
            when 'Road-650' then 'R750'
            when 'Touring-1000' then 'T1000'
            else left(model, 1) || right(model, 3)
        end || ' ' || region,
        (calendar_year::integer * 100) + "month"::integer,
        calendar_year,
        "month",
        public.udf_build_ISO8601_date(calendar_year::integer, "month"::integer, 25);
