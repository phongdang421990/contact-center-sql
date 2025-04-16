USE [pcce2_awdb]
GO
/****** Object:  StoredProcedure [dbo].[Vietin_Call_Monitor_Chart]    Script Date: 9/8/2023 5:20:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[Vietin_Call_Monitor_Chart]
AS
BEGIN
IF OBJECT_ID('tempdb.dbo.##TMP_TABLE_TCD_3_VTB', 'U') IS NOT NULL
	drop table ##TMP_TABLE_TCD_3_VTB		-- -- Toàn bộ dữ liệu inbound vào hệ thống theo đầu số 8868
IF OBJECT_ID('tempdb.dbo.##FINAL_CM_3_VTB', 'U') IS NOT NULL
	drop table ##FINAL_CM_3_VTB		-- -- Table chứa 3 cột 42 - 43,1 - 2
IF OBJECT_ID('tempdb.dbo.##FINAL2_CM_3_VTB', 'U') IS NOT NULL
	drop table ##FINAL2_CM_3_VTB	

declare @now DateTime = GetDate()
declare @nowDate DateTime = convert(date, @now)
declare @table table(Period varchar(10), QueueCall int, HoldCall int, ReceivedCall int)
declare @table_hold table(Period varchar(10), HoldCall int)

-- ---------------------------------------------
select DATEADD(hour,7,StartDateTimeUTC) "StartDateTime", ANI, DigitsDialed, DNIS, PeripheralCallType, RouterCallKeySequenceNumber "RouterCallKeySN"
	, CallDispositionFlag, CallDisposition, Duration, DelayTime, LocalQTime, RingTime, TalkTime, HoldTime, NetQTime, NetworkTime, TimeToAband, WorkTime
	, DateTime "EndDateTime", AgentSkillTargetID, CallTypeID
	, Variable1 "var1", Variable5 "var5", Variable6 "var6", Variable7 "var7", Variable8 "var8", Variable9 "var9", Variable10 "var10"
	, CallGUID, RouterCallKeyDay, RouterCallKey, concat(RouterCallKeyDay,RouterCallKey) "RouterCallId", PeripheralCallKey
	, SkillGroupSkillTargetID, InstrumentPortNumber, AgentPeripheralNumber, PrecisionQueueID, CallTypeReportingDateTime
	, concat(RouterCallKeyDay,RouterCallKey,RouterCallKeySequenceNumber) "RouterCallIdSn"
into ##TMP_TABLE_TCD_3_VTB
from Termination_Call_Detail (nolock)
where DATEADD(HOUR,7,StartDateTimeUTC) between @nowDate and @now
	and LEN(ANI) > 7 and (DigitsDialed = '8868' or PeripheralCallType = 4)
	and not (CallDisposition = 52 and Duration < 4)
	and not (RIGHT(Variable9,3) = 'pcs')

select t1.StartDateTime, t1.ANI, t1.DigitsDialed, t1.DNIS, t1.RouterCallKeyDay "RouterCallKeyDay", t1.RouterCallKey "RouterCallKey" , t1.var9 "var9"
	, t1.Duration "Duration", t1.DelayTime "DelayTime", t1.TalkTime "TalkTime", t1.TimeToAband "TimeToAband", t1.CallTypeID "CallTypeID", t1.RouterCallId
	, t2.var5 "var5_2", t2.var6 "var6_2", t2.var7 "var7_2", t2.var8 "var8_2", t2.var9 "var9_2", t2.var10 "var10_2"
	, t2.PeripheralCallType "PeripheralCallType_2", t2.CallDispositionFlag "CallDispositionFlag_2", t2.CallDisposition "CallDisposition_2"
	, t2.CallTypeID "CallTypeID_2"
	, t3.var5 "var5_3", t3.var6 "var6_3", t3.var7 "var7_3", t3.var8 "var8_3", t3.var9 "var9_3", t3.var10 "var10_3"
	, t3.PeripheralCallType "PeripheralCallType_3", t3.CallDispositionFlag "CallDispositionFlag_3", t3.CallDisposition "CallDisposition_3"
	, t3.AgentPeripheralNumber "AgentPeripheralNumber_3", t3.PrecisionQueueID "PrecisionQueueID_3", t3.SkillGroupSkillTargetID "SkillGroupSkillTargetID_3"
	, t3.DNIS "DNIS_3", t3.ANI "ANI_3", t3.CallTypeID "CallTypeID_3", t3.AgentSkillTargetID "AgentSkillTargetID_3"
into ##FINAL_CM_3_VTB 
from ##TMP_TABLE_TCD_3_VTB t1
	join ##TMP_TABLE_TCD_3_VTB t2 on t1.CallGUID = t2.CallGUID 
	and t1.PeripheralCallType = 42 and t2.PeripheralCallType in (43,1)
	and t2.RouterCallIdSn in (
		select max(RouterCallIdSn) from ##TMP_TABLE_TCD_3_VTB where PeripheralCallType in (43,1) group by CallGUID
	)
	left join (
		select * from ##TMP_TABLE_TCD_3_VTB where RouterCallIdSn in (select max(RouterCallIdSn) from ##TMP_TABLE_TCD_3_VTB where PeripheralCallType in (2,4) group by RouterCallId)
	) t3 on t1.RouterCallId = t3.RouterCallId
	
 /*
	-- ######################## FINAL TABLE , can create view at here ################################################
 */

-- -- ##FINAL2_CM_3_VTB TABLE , GOM LẠI CÁC THÔNG TIN T1,T2 VÀ T3 THÀNH 1 TABLE
select *
into ##FINAL2_CM_3_VTB
from (
	---- 42,1
	select StartDateTime, ANI, DNIS, IIF(CallTypeID = CallTypeID_2,var9,var9_2) "var9", Duration, DelayTime, TalkTime, var10_2
		, case when var10_2 is null and CallDisposition_2 in (13,52) and CallTypeID_2 = 5000 then 'Hangup in IVR'	-- 52 VRU hangup, CallType = CallType_2 = 5000 là ivr
			--when var10_2 is null and CallDisposition_2 in (13,52) and CallTypeID_2 != 5000 then 'Self Service'	-- thường là các CallTypeID của Self Service
			when var10_2 is null and CallDisposition_2 in (13,52) and CallTypeID_2 in (5038,5039,5040,5041,5042,5043,5044,5045) then 'Self Service'
			when var10_2 is not null and CallDisposition_2 = 13 then 'Hangup in Queue IVR'
			else 'Other' end "CallDispositionName"
		, RouterCallKeyDay, RouterCallKey, '' "AgentSkillTargetID", '' "PrecisionQueueID"
		, CallDispositionFlag_2 "CallDispositionFlag", CallDisposition_2 "CallDisposition", RouterCallId, var6_2 "var6"
	from ##FINAL_CM_3_VTB
	where ANI_3 is null and PeripheralCallType_2 = 1
	union all

	-- 42-43
	select StartDateTime, ANI, DNIS, var9_2 "var9", Duration, DelayTime, TalkTime, var10_2
		, case when (CallDispositionFlag_2 = 2 and CallDisposition_2 = 2) then 'Abandoned in Local Queue'
			when (CallDispositionFlag_2 = 1 and CallDisposition_2 = 13 and CallTypeID_2 != 5000) then 'Miss Call in Queue'	-- nếu thêm điều kiện CallTypeID = CallTypeID_2 thì sẽ có Other, ko hiểu tại sao có 2 cái CallTypeID != CallTypeID_2 sẽ rơi vào trường hợp nào
			when (CallDispositionFlag_2 = 1 and CallDisposition_2 in (13,52) and CallTypeID_2 = 5000) then 'Hangup in IVR'	-- 43 nhưng có CallTypeID = 5000 và ko xác định đc skill vào
			else 'Other' end "CallDispositionName"
		, RouterCallKeyDay, RouterCallKey, '' "AgentSkillTargetID", '' "PrecisionQueueID"
		, CallDispositionFlag_2 "CallDispositionFlag", CallDisposition_2 "CallDisposition", RouterCallId, var6_2 "var6"			-- them để test 
	from ##FINAL_CM_3_VTB
	where ANI_3 is null and PeripheralCallType_2 = 43
		--and var6_2 != 'CallBack'	-- remove CallBack
	union all

	-- 42,1,2 và 42,43,2
	select StartDateTime, ANI, DNIS, var9_2 "var9", Duration, DelayTime, TalkTime, var10_2
		, case when CallDispositionFlag_3 = 2 and CallDisposition_3 = 3 then 'Abandoned Ring'	-- 42,1,2
			when CallDispositionFlag_3 = 3 and CallDisposition_3 = 7 then 'Short Call'
			--when CallDispositionFlag_3 = 1 and CallDisposition_3 = 6 then 'Abandoned Agent Terminal'
			--when CallDispositionFlag_3 = 1 and CallDisposition_3 in (28,29) then 'Transfer'
			when CallDisposition_3 = 1 then 'Abandoned in Network'		--CallDispositionFlag_3 = 7 and 
			when CallDispositionFlag_3 = 1 and CallDisposition_3 in (13,6,28,29,30) then 'Served by Agent'	--28,29: transfer; 30: conference; 6: Abandoned in Agent
			when CallDispositionFlag_3 = 6 and CallDisposition_3 = 19 then 'Ring No Answer'
			when CallDispositionFlag_3 = 4 and CallDisposition_3 = 60 then 'Network Error'
			else 'Other' end "CallDispositionName"
		, RouterCallKeyDay, RouterCallKey, AgentSkillTargetID_3 "AgentSkillTargetID", PrecisionQueueID_3 "PrecisionQueueID"
		, CallDispositionFlag_3 "CallDispositionFlag", CallDisposition_3 "CallDisposition", RouterCallId, var6_2 "var6"
	from ##FINAL_CM_3_VTB
	where ANI_3 is not null and PeripheralCallType_2 in (1,43) 
		--and var6_2 != 'CallBack'	-- remove CallBack
) tmp
-- ---------------------------------------------

declare @count int = 0
declare @targer int = iif(DATEPART(MINUTE, @now) > 0, DATEPART(HOUR, @now)+1, DATEPART(HOUR, @now))
declare @begin DateTime
declare @end DateTime

while @count < @targer
begin
	set @begin = DATEADD(HOUR, @count, DATEADD(MINUTE, 0, DATEADD(SECOND, 0, @nowDate)))
	set @end = DATEADD(HOUR, @count+1, DATEADD(MINUTE, 0, DATEADD(SECOND, 0, @nowDate)))
	insert into @table
	select 
		case
			when  @targer = DATEPART(HOUR, @now)+1 and @count = @targer - 1 
			then concat(@count,'h-',DATEPART(HOUR, @now),'h',DATEPART(MINUTE, @now))
			else concat(@count,'h-',(@count+1),'h')
		end
		,sum(iif(CallDispositionName in ('Hangup in IVR', 'Self Service', 'Abandoned in Local Queue','Abandoned in Network','Abandoned Ring','Miss Call in Queue','Ring No Answer'), 1, 0))
		, 0
		, count(1)
	from ##FINAL2_CM_3_VTB
	where StartDateTime >= @begin and StartDateTime < @end

	insert into @table_hold
	select 
		case
			when  @targer = DATEPART(HOUR, @now)+1 and @count = @targer - 1 
			then concat(@count,'h-',DATEPART(HOUR, @now),'h',DATEPART(MINUTE, @now))
			else concat(@count,'h-',(@count+1),'h')
		end
		, sum(HoldTime)
	from ##TMP_TABLE_TCD_3_VTB
	where StartDateTime >= @begin and StartDateTime < @end and HoldTime > 0

	set @count = @count + 1
end

select t1.Period
	,t1.QueueCall
	,t2.HoldCall
	,t1.ReceivedCall
from @table AS t1
Join @table_hold AS t2
on t1.Period = t2.Period

IF OBJECT_ID('tempdb.dbo.##TMP_TABLE_TCD_3_VTB', 'U') IS NOT NULL
	drop table ##TMP_TABLE_TCD_3_VTB		-- -- Toàn bộ dữ liệu inbound vào hệ thống theo đầu số 8868
IF OBJECT_ID('tempdb.dbo.##FINAL_CM_3_VTB', 'U') IS NOT NULL
	drop table ##FINAL_CM_3_VTB		-- -- Table chứa 3 cột 42 - 43,1 - 2
IF OBJECT_ID('tempdb.dbo.##FINAL2_CM_3_VTB', 'U') IS NOT NULL
	drop table ##FINAL2_CM_3_VTB
END