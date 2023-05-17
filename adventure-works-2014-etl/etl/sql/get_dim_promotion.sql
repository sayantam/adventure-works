-- MS SQL
select PromotionAlternateKey,
       EnglishPromotionName,
       SpanishPromotionName,
       FrenchPromotionName,
       DiscountPct,
       EnglishPromotionType,
       SpanishPromotionType,
       FrenchPromotionType,
       EnglishPromotionCategory,
       SpanishPromotionCategory,
       FrenchPromotionCategory,
       StartDate,
       EndDate,
       MinQty,
       MaxQty
from dbo.DimPromotion;
