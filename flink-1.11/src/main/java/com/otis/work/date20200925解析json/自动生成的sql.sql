from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01E.PD01EH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01ER03,PD01ED01,PD01EJ01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF08Z.PF08ZH     as data
from PAHPF08
)t1,unnest(t1.data) as info(PF08ZD01,PF08ZQ01,PF08ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF07Z.PF07ZH     as data
from PPQPF07
)t1,unnest(t1.data) as info(PF07ZD01,PF07ZQ01,PF07ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF06Z.PF06ZH     as data
from PBSPF06
)t1,unnest(t1.data) as info(PF06ZD01,PF06ZQ01,PF06ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF05Z.PF05ZH     as data
from PHFPF05
)t1,unnest(t1.data) as info(PF05ZD01,PF05ZQ01,PF05ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF04Z.PF04ZH     as data
from PAPPF04
)t1,unnest(t1.data) as info(PF04ZD01,PF04ZQ01,PF04ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF03Z.PF03ZH     as data
from PCEPF03
)t1,unnest(t1.data) as info(PF03ZD01,PF03ZQ01,PF03ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF02Z.PF02ZH     as data
from PCJPF02
)t1,unnest(t1.data) as info(PF02ZD01,PF02ZQ01,PF02ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PF01Z.PF01ZH     as data
from POTPF01
)t1,unnest(t1.data) as info(PF01ZD01,PF01ZQ01,PF01ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PE01Z.PE01ZH     as data
from PNDPE01
)t1,unnest(t1.data) as info(PE01ZD01,PE01ZQ01,PE01ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD03Z.PD03ZH     as data
from PCRPD03
)t1,unnest(t1.data) as info(PD03ZD01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD02Z.PD02ZH     as data
from PCAPD02
)t1,unnest(t1.data) as info(PD02ZD01,PD02ZQ01,PD02ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01Z.PD01ZH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01ZD01,PD01ZQ01,PD01ZR01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01H.PD01HH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01HJ01,PD01HR01,PD01HR02,PD01HJ02)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01G.PD01GH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01GR01,PD01GD01)

from(
select
report_id 			as report_id,
SID 				as SID,
STATISTICS_DT 		as STATISTICS_DT,
PD01F.PD01FH     as data
from PDAPD01
)t1,unnest(t1.data) as info(PD01FD01,PD01FR01,PD01FS02,PD01FJ01,PD01FQ01)

