-- PG SQL
insert into public.dim_promotion (
    promotion_alternate_key,
    english_promotion_name,
    spanish_promotion_name,
    french_promotion_name,
    discount_pct,
    english_promotion_type,
    spanish_promotion_type,
    french_promotion_type,
    english_promotion_category,
    spanish_promotion_category,
    french_promotion_category,
    start_date,
    end_date,
    min_qty,
    max_qty
) values (
    %(promotion_alternate_key)s,
    %(english_promotion_name)s,
    %(spanish_promotion_name)s,
    %(french_promotion_name)s,
    %(discount_pct)s,
    %(english_promotion_type)s,
    %(spanish_promotion_type)s,
    %(french_promotion_type)s,
    %(english_promotion_category)s,
    %(spanish_promotion_category)s,
    %(french_promotion_category)s,
    %(start_date)s,
    %(end_date)s,
    %(min_qty)s,
    %(max_qty)s
) on conflict do nothing;
