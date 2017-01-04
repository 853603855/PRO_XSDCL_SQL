-- ----------------------------
-- Procedure structure for pro_xsd_zddx
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsd_zddx`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsd_zddx`(in v_zbid varchar(100),in v_ywxt varchar(20),in cs_flbm varchar(19),in cs_spmc varchar(200),in cs_ggxh varchar(100),
in cs_slv decimal(10,6),in cs_jldw varchar(100),in cs_hsbz varchar(1),in cs_fphxz varchar(1),in cs_spbh varchar(20),
in cs_spsm varchar(100),in cs_xsyh varchar(1),in cs_yhsm varchar(100),in cs_lslvbs varchar(1))
BEGIN

	declare fd_cursor_flag int default 0;
    
	declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
    
    declare v_zd_je decimal(16,2);
    declare v_zd_je_2 decimal(16,2);
    declare v_cz decimal(16,2);
    
    declare v_id varchar(100);
    declare v_fd_je decimal(16,2);
    declare v_flbm varchar(19);
	declare	v_spmc varchar(200);
	declare	v_ggxh varchar(100);
	declare	v_slv decimal(10,6);
	declare	v_jldw varchar(100);
	declare	v_hsbz varchar(1);
	declare	v_fphxz varchar(1);
    
	declare v_spbh varchar(20);
    declare v_spsm varchar(100);
    declare v_xsyh varchar(1);
    declare v_yhsm varchar(100);
    declare v_lslvbs varchar(1);
    
    declare v_zdjh varchar(100);
    declare v_fdjh varchar(100);
    
    declare fd_cursor cursor for
    select tmp_id,tmp_fd_hjje,tmp_flbm,tmp_spmc,tmp_ggxh,tmp_slv,tmp_jldw,tmp_hsbz,tmp_fphxz,
    tmp_spbh,tmp_spsm,tmp_xsyh,tmp_yhsm,tmp_lslvbs
    from tmp_xsd_zd_zpdx
    where tmp_flbm=cs_flbm and tmp_spmc=cs_spmc and (case when cs_ggxh is NULL then tmp_ggxh is NULL else tmp_ggxh=cs_ggxh end)
    and tmp_slv=cs_slv and (case when cs_jldw is NULL then tmp_jldw is NULL else tmp_jldw=cs_jldw end) and tmp_hsbz=cs_hsbz     
    and tmp_fphxz=cs_fphxz and (case when cs_spbh is null then tmp_spbh is null else tmp_spbh = cs_spbh end)
	and (case when cs_spsm is null then tmp_spsm is null else tmp_spsm = cs_spsm end)
	and (case when cs_xsyh is null then tmp_xsyh is null else tmp_xsyh = cs_xsyh end)
	and (case when cs_yhsm is null then tmp_yhsm is null else tmp_yhsm = cs_yhsm end)
	and (case when cs_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = cs_lslvbs end) and tmp_dxzt = '0' order by tmp_fd_hjje desc;
    
    -- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_xsd_zd_zpdx','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_xsd_zd_zpdx','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单——正单抵消: ',error_xx),
		sysdate(),concat('正单ID',v_zbid,'分类编码：',cs_flbm,'，商品名称：',cs_spmc,'，规格类型：',cs_ggxh,'，税率：',cs_slv,'，计量单位：',cs_jldw,
        '，含税标志：',cs_hsbz,'，发票行性质：',cs_fphxz),concat('商品编号：',cs_spbh,'，商品税目',cs_spsm,'，享受优惠',cs_xsyh,'，优惠说明',cs_yhsm,'，零税率表示',cs_lslvbs));
		commit;
	end;

	-- 当游标到最后一条时，终止循环
	declare continue handler for not found set fd_cursor_flag =1;
	set autocommit=0;
    
    select sum(tmp_je) into v_zd_je from tmp_mc_zdcl_zp_sx;
    select hjje into v_zd_je_2 from djgl_xsdxx where id=v_zbid;
    
	open fd_cursor;
    loop_fd:loop
		fetch fd_cursor 
        into v_id,v_fd_je,v_flbm,v_spmc,v_ggxh,v_slv,v_jldw,v_hsbz,v_fphxz,
			 v_spbh,v_spsm,v_xsyh,v_yhsm,v_lslvbs;
        
        if fd_cursor_flag=1 then
			leave loop_fd;
		end if;
        
        -- 判断此负单是否已被存于负单表中，若有，将其删除；
        if exists(select * from djgl_fdxx where xsdid = v_id) then
        
			delete from djgl_fdxx where xsdid = v_id;
        
        end if;
        
        if v_fd_je is NULL then
        
			set v_fd_je = 0;
        
        end if;
        
        set v_zd_je = v_zd_je + v_fd_je;
        
        if v_zd_je > 0 then
        
			-- 注：临时表tmp_xsd_zd_zpdx的tmp_dxzt代表当前负单的状态：
            -- 0：未操作；1：可结算；2：不可结算；3：已存入负单和待开关系;4:已参与明细结算(避免待开状态下明细相同导致重复抵消)
			update tmp_xsd_zd_zpdx set tmp_dxzt = '1' where tmp_id = v_id;
            
            -- 在销售单表中，xsdzt为8代表状态“已结算”
            update djgl_xsdxx set xsdzt = '8' where id = v_id;
            
            select djbh into v_zdjh from djgl_xsdxx where id=v_zbid;
            
            select djbh into v_fdjh from djgl_xsdxx where id=v_id;
            
            -- 存入抵消关系
            insert into djgl_dxgx(ywxt,fdjh,fdje,zdjh,zdje,dxrq) select v_ywxt,v_fdjh,v_fd_je,v_zdjh,v_zd_je_2,now();
            
		elseif v_zd_je = 0 then
        
			update tmp_xsd_zd_zpdx set tmp_dxzt = '1' where tmp_id = v_id;
            
            -- update djgl_xsdxx set xsdzt='8' where id=v_zbid;
            
            update djgl_xsdxx set xsdzt='8' where id=v_id;
            
            select djbh into v_zdjh from djgl_xsdxx where id=v_zbid;
            
            select djbh into v_fdjh from djgl_xsdxx where id=v_id;
            
            -- 存入抵消关系
            insert into djgl_dxgx(ywxt,fdjh,fdje,zdjh,zdje,dxrq) select v_ywxt,v_fdjh,v_fd_je,v_zdjh,v_zd_je_2,now();
        
        elseif v_zd_je < 0 then
        
			update tmp_xsd_zd_zpdx set tmp_dxzt = '2' where tmp_id = v_id;
            
            insert into djgl_fdxx(xsdid,lphm,lpdm,lrrq)
			select xsd.id,pjgx.fphm,pjgx.fpdm,now() from djgl_xsdxx xsd left join kpgl_xxfp_pjgx pjgx on (xsd.ydh=pjgx.xsdjbh and xsd.ywxt=pjgx.ywxt)
            where xsd.id=v_id;
        
        end if;
        
	end loop;
    close fd_cursor;
    
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsd_zd_zpdx','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单—正单抵消: 成功',sysdate(),concat('正单ID',v_zbid,'分类编码：',cs_flbm,'，商品名称：',cs_spmc,'，规格类型：',cs_ggxh,'，税率：',
    cs_slv,'，计量单位：',cs_jldw,'，含税标志：',cs_hsbz,'，发票行性质：',cs_fphxz),concat('商品编号：',cs_spbh,'，商品税目',cs_spsm,'，享受优惠',cs_xsyh,'，优惠说明',cs_yhsm,'，零税率表示',cs_lslvbs));
    
    commit;
    
END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_mx_cf
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_mx_cf`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_mx_cf`(in v_id varchar(100),in v_qdbz varchar(1),in v_dzxezp decimal(16,2),
in v_fplx varchar(2),in v_ywxt varchar(20))
BEGIN
	declare zpcf_mx_flag int default 0;
    
	-- 用于异常排查
	declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
	declare mc_zp_cursor_flag int default 0;
            
	-- 用于游标zpcf_mx_cursor
    declare v_mxid bigint(100);
    declare v_mxje decimal(16,2);
    declare v_mxdj decimal(36,18);
	declare v_mxsl decimal(36,18);
    -- 用于将明细信息进行分组
	declare v_flbm varchar(19);
	declare v_spmc varchar(200);
	declare v_ggxh varchar(100);
	declare v_slv decimal(10,6);
	declare v_jldw varchar(100);
	declare v_hsbz varchar(1);
	declare v_fphxz varchar(1);
    declare v_spbh varchar(20);
    declare v_spsm varchar(100);
    declare v_xsyh varchar(1);
    declare v_yhsm varchar(100);
    declare v_lslvbs varchar(1);
    
    -- 用于累计（减）明细金额和限额的差值
    declare v_mxcz decimal(16,2);
    
    -- 明细序号
    declare v_fpmxxh varchar(100);
    
    -- 存取ZBID——主表ID()
    -- 用于明细金额超限
    declare v_zbid varchar(100);
    -- 用于明细金额小于限额
    declare v_zbid_2 varchar(100);
    -- 用于累计金额超限，修改明细
    declare v_zbid_3 varchar(100);
    
    -- 用于获取最大的明细序列(在mysql中select的同时不能update)
    declare v_n varchar(100);
            
	-- 清单标志所需计数器
	declare v_ts int(5);
            
	declare v_fd_je decimal(16,2);
    
    -- 用于合计明细金额小于限额时的各明细之和，用于判断累加时是否超限
    declare v_mx_ljje decimal(16,2);
    
    -- 用于合计明细金额小于限额时的各明细之和
    declare v_mx_hjje decimal(16,2);
    
    -- 用于统计税率
    declare v_hjse decimal(16,2);
    
    -- 待开状态码
    declare v_dkzt varchar(1);
            
	-- 在拆分前做抵消的话，需在游标内加入另外七个字段来寻找负单的明细。（由于明细会不同，并非每个明细都会有抵消操作）
	declare zpcf_mx_cursor cursor for
	select mx.id,je,dj,sl,flbm,spmc,ggxh,slv,jldw,hsbz,fphxz,spbh,spsm,xsyh,yhsm,lslvbs
    from djgl_xsdxx_mx mx where xsdjid=v_id;
    
    -- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
		GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_mx_cf','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_mx_cf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单—正单处理—明细拆分: ',error_xx),
		sysdate(),concat('销售单ID：',v_id,'清单标志：',v_qdbz,'销售单限额',v_dzxezp,'发票类型',v_fplx,'业务系统：',v_ywxt),null);
		commit;
	end;
            
	-- 当游标循环遍历结束，结束游标
	declare continue handler for not found set zpcf_mx_flag =1;
	set autocommit=0;
    
    set v_fpmxxh=1;
    
    set v_mx_ljje=0;
    
	SELECT concat('XSD-',uuid()) into v_zbid_2;
    
    -- 获取清单标志，判断可输入明细条数（6000 or 8）
	if v_qdbz='0' then
		set v_ts=8;                    
	else                
		set v_ts=6000;                    
	end if;
    
    -- 待开状态码初始为零
	set v_dkzt='0';
    
	open zpcf_mx_cursor;
	loop_zpcf_mx:loop
		fetch zpcf_mx_cursor into v_mxid,v_mxje,v_mxdj,v_mxsl,v_flbm,v_spmc,v_ggxh,v_slv,v_jldw,v_hsbz,v_fphxz,
        v_spbh,v_spsm,v_xsyh,v_yhsm,v_lslvbs;
            
		if zpcf_mx_flag =1 then
				
			leave loop_zpcf_mx;
                
		end if;
        
        -- 重新校验单价
        if v_mxsl is not null then
			set v_mxdj=v_mxje/v_mxsl;
		else
			set v_mxdj=null;
        end if;
                
		-- 先插入对应的明细
		insert into tmp_mc_zdcl_zp_sx(tmp_je) select je from djgl_xsdxx_mx where id = v_mxid;
		-- 获取抵消
		-- 销售单正单处理-专票抵消
		call pro_xsd_zddx(v_id,v_ywxt,v_flbm,v_spmc,v_ggxh,v_slv,v_jldw,v_hsbz,v_fphxz,v_spbh,v_spsm,v_xsyh,v_yhsm,v_lslvbs);
        
        delete from tmp_mc_zdcl_zp_sx;
        
		-- 检测本条负单是否有负担需要做抵消
        if exists(select * from tmp_xsd_zd_zpdx 
			where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
			and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
			and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
			and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
			and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
			and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
			and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1) then
                    
			select sum(tmp_fd_hjje) into v_fd_je from tmp_xsd_zd_zpdx 
			where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
			and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
			and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
			and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
			and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
			and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
			and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1;
                   
			update tmp_xsd_zd_zpdx set tmp_dxzt = '4' where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
			and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
			and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
			and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
			and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
			and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
			and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1;
            
		else
                    
			set v_fd_je=0;
                    
		end if;
                
		set v_mxje=v_mxje+v_fd_je;
                
		set v_mxcz=v_mxje;   
        
        -- 当明细金额为0时，本条明细不必再存入待开明细表(即跳过本条明细)
        if v_mxcz = 0 then
			iterate loop_zpcf_mx;
        end if;
                
		-- 断点15						
		if v_mxcz > v_dzxezp then                 
                    
			mxcf:loop
                    
				if v_mxcz > v_dzxezp then 
                
					set v_dkzt='1';
				
					SELECT concat('XSD-',uuid()) into v_zbid;
                    
                    -- 每向待开表插入一条数据，需向关系表插入一条关联信息
                    insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid,v_id,now());
                    
                    -- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
                    if v_fplx = '0' then 
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid,'5000','4',now(); 
                        
					else
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid,'50001','4',now(); 
                    
                    end if;
                            
					if v_qdbz = '0' then 
                            
						insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_zbid,v_fplx,'1',v_dzxezp,slv,v_dzxezp*slv,spbh,spmc,spsm,ggxh,jldw,v_dzxezp/v_mxdj,dj,now(),lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,
						yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;	
                        
                        -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_mx表中获取
						select se into v_hjse from kpgl_dk_xxfp_mx where zbid=v_zbid;
                                                        
					end if;
                            
					if v_qdbz = '1' then 
                            
						insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_zbid,v_fplx,'1',v_dzxezp,slv,v_dzxezp*slv,spbh,spmc,spsm,ggxh,jldw,v_dzxezp/v_mxdj,dj,now(),lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,
						yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;	
                        
                        -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_qd表中获取
						select se into v_hjse from kpgl_dk_xxfp_qd where zbid=v_zbid;
                        
                        -- 差税额和金额
                                
						-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
						insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
						values (v_zbid,'(详见销货清单)',now(),v_fplx,'1','0','1');
                                                        
					end if;
                    
                    insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
					select v_zbid,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,v_dzxezp,v_hjse,bz,
					fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
					from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
					where xsd.id=v_id;
                            										
					set v_mxcz = v_mxcz - v_dzxezp;
                            
					-- 存入负单和待开ID间的关系
                    if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
						insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
                        select v_zbid,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                    
						-- 在负单关系存入后，将其状态改为3
                        update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
                    end if;                                
                        
				else
                        
					SELECT concat('XSD-',uuid()) into v_zbid;
                    
                    -- 每向待开表插入一条数据，需向关系表插入一条关联信息
                    insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid,v_id,now());
                    
                    -- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
                    if v_fplx = '0' then 
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid,'5000','4',now(); 
                        
					else
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid,'50001','4',now(); 
                    
                    end if;
                            
					if v_qdbz = '0' then 
                            
						insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_zbid,v_fplx,'1',v_mxcz,slv,v_mxcz*slv,spbh,spmc,spsm,ggxh,jldw,v_mxcz/v_mxdj,dj,now(),lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,
						yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;	
                        
                        -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_mx表中获取
						select se into v_hjse from kpgl_dk_xxfp_mx where zbid=v_zbid;
                                                        
					end if;
                            
					if v_qdbz = '1' then 
                            
						insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_zbid,v_fplx,'1',v_mxcz,slv,v_mxcz*slv,spbh,spmc,spsm,ggxh,jldw,v_mxcz/v_mxdj,dj,now(),lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,
						yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;	
                        
                        -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_qd表中获取
						select se into v_hjse from kpgl_dk_xxfp_qd where zbid=v_zbid;
                        
                        -- 差税额和金额
                                
						-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
						insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
						values (v_zbid,'(详见销货清单)',now(),v_fplx,'1','0','1');
                                                        
					end if;
                    
                    insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
					select v_zbid,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,v_mxcz,v_hjse,bz,
					fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
					from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
					where xsd.id=v_id;
                    
                    -- 存入负单和待开ID间的关系
                    if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
						insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
                        select v_zbid,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                    
						-- 在负单关系存入后，将其状态改为3
                        update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
                    end if; 
                            
					leave mxcf;
                        
				end if;						
                    
			end loop;
                
		else
        
			set v_mx_ljje = v_mx_ljje + v_mxje;
                            
			if v_qdbz = '0' then 
                            
				insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
				lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
				select v_zbid_2,v_fplx,v_fpmxxh,v_mxcz,slv,v_mxcz*slv,spbh,spmc,spsm,ggxh,jldw,v_mxcz/v_mxdj,dj,now(),lrryid,lrrymc,
				lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,
				yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;	
                                                        
			end if;
                            
			if v_qdbz = '1' then 
                            
				insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
				lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
				select v_zbid_2,v_fplx,v_fpmxxh,v_mxcz,slv,v_mxcz*slv,spbh,spmc,spsm,ggxh,jldw,v_mxcz/v_mxdj,dj,now(),lrryid,lrrymc,
				lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,
				yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;	
                                                        
			end if;
                            
			set v_fpmxxh = v_fpmxxh + 1;
            
            -- 当明细金额达到上限是，处理销售单（存入待开表）
            if v_mx_ljje > v_dzxezp then 
            
				set v_dkzt='1';
            
				if (select count(*) from kpgl_dk_xxfp_mx where zbid=v_zbid_2) > 0 then
                
					-- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_mx表中获取
                    select sum(se) into v_hjse from kpgl_dk_xxfp_mx where zbid=v_zbid_2;
            
					insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
					select v_zbid_2,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,(v_mx_ljje-v_mxje),v_hjse,bz,
					fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
					from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
					where xsd.id=v_id;
                    
                    set v_zbid_3= v_zbid_2;
                    
                    -- 每向销售单存入一条信息，需扣除存入的金额
                    set v_mx_ljje = v_mxje;
                    
                    -- 每向待开表插入一条数据，需向关系表插入一条关联信息
                    insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid_2,v_id,now());
                    
                    -- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
                    if v_fplx = '0' then 
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'5000','4',now(); 
                        
					else
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'50001','4',now(); 
                    
                    end if;
                    
				end if;
                    
				if (select count(*) from kpgl_dk_xxfp_qd where zbid=v_zbid_2) > 0 then
                
					-- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_qd表中获取
                    select sum(se) into v_hjse from kpgl_dk_xxfp_qd where zbid=v_zbid_2;
            
					insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
					select v_zbid_2,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,(v_mx_ljje-v_mxje),v_hjse,bz,
					fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
					from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
					where xsd.id=v_id;
                    
                    set v_zbid_3= v_zbid_2;
                    
                    -- 每向销售单存入一条信息，需扣除存入的金额
                    set v_mx_ljje = v_mxje;
                    
                    -- 每向待开表插入一条数据，需向关系表插入一条关联信息
                    insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid_2,v_id,now());
                    
                    -- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
                    if v_fplx = '0' then 
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'5000','4',now(); 
                        
					else
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'50001','4',now(); 
                    
                    end if;
                    
                    if v_qdbz = '1' then
        
						-- 差税额和金额
                        
						-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
						insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
						values (v_zbid_2,'(详见销货清单)',now(),v_fplx,'1','0','1');
                        
					end if;
                
                end if;
                
                -- 重新赋值
				SELECT concat('XSD-',uuid()) into v_zbid_2;
                
                if v_qdbz = '1' then 
					
                    select max(fpmxxh) into v_n from kpgl_dk_xxfp_qd where zbid=v_zbid_3;
                    
					update kpgl_dk_xxfp_qd set zbid = v_zbid_2,fpmxxh ='1'
                    where fpmxxh=v_n and zbid=v_zbid_3;
                
                else
					
                    select max(fpmxxh) into v_n from kpgl_dk_xxfp_mx where zbid=v_zbid_3;
                    
                    update kpgl_dk_xxfp_mx set zbid = v_zbid_2,fpmxxh ='1' 
                    where fpmxxh=v_n and zbid=v_zbid_3;
                
                end if;
                
                set v_fpmxxh = 2;
                
                -- 存入负单和待开ID间的关系
				if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
					insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
					select v_zbid_2,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                    
					-- 在负单关系存入后，将其状态改为3
					update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
				end if; 
            
            end if;
            
            -- 当明细达过上限时，处理销售单（存入待开表）
			if v_ts  = v_fpmxxh - 1 then 
            
				set v_dkzt='1';
            
				if (select count(*) from kpgl_dk_xxfp_mx where zbid=v_zbid_2) > 0 then
            
					select sum(je) into v_mx_hjje from kpgl_dk_xxfp_mx where zbid=v_zbid_2;
                    
                    -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_mx表中获取
                    select sum(se) into v_hjse from kpgl_dk_xxfp_mx where zbid=v_zbid_2;
            
					insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
					select v_zbid_2,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,v_mx_hjje,v_hjse,bz,
					fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
					from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
					where xsd.id=v_id;
                    
                    -- 每向销售单存入一条信息，需扣除存入的金额
                    set v_mx_ljje = v_mx_ljje - v_mx_hjje;
                    
                    -- 每向待开表插入一条数据，需向关系表插入一条关联信息
					insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid_2,v_id,now());
                    
					-- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
					if v_fplx = '0' then 
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'5000','4',now(); 
                        
					else
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'50001','4',now(); 
                    
					end if;
                    
				end if;
                    
				if (select count(*) from kpgl_dk_xxfp_qd where zbid=v_zbid_2) > 0 then
                
					select sum(je) into v_mx_hjje from kpgl_dk_xxfp_qd where zbid=v_zbid_2;
                    
                    -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_qd表中获取
                    select sum(se) into v_hjse from kpgl_dk_xxfp_qd where zbid=v_zbid_2;
            
					insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
					select v_zbid_2,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,v_mx_hjje,v_hjse,bz,
					fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
					from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
					where xsd.id=v_id;
                    
                    -- 每向销售单存入一条信息，需扣除存入的金额
                    set v_mx_ljje = v_mx_ljje - v_mx_hjje;
                    
                    -- 每向待开表插入一条数据，需向关系表插入一条关联信息
					insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid_2,v_id,now());
                    
					-- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
					if v_fplx = '0' then 
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'5000','4',now(); 
                        
					else
                    
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'50001','4',now(); 
                    
					end if;
                    
                    if v_qdbz = '1' then
        
						-- 差税额和金额
                        
						-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
						insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
						values (v_zbid_2,'(详见销货清单)',now(),v_fplx,'1','0','1');
                        
					end if;
                
                end if;
                                
				set v_fpmxxh=1;
                
                -- 重新赋值
                SELECT concat('XSD-',uuid()) into v_zbid_2;
                  
				-- 存入负单和待开ID间的关系
				if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
					insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
					select v_zbid_2,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                    
					-- 在负单关系存入后，将其状态改为3
					update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
				end if; 
				
			end if;
            
		end if;      	                
                		                
	end loop;
	close zpcf_mx_cursor;
    
    -- 处理明细金额小于限额的明细对应的销售单
    if v_qdbz = '1' then
    
		if (select count(*) from kpgl_dk_xxfp where id=v_zbid_2) = 0 and (select count(*) from kpgl_dk_xxfp_qd  where zbid=v_zbid_2) > 0 then
    
			select sum(je) into v_mx_hjje from kpgl_dk_xxfp_qd where zbid=v_zbid_2;
            
            -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_qd表中获取
			select sum(se) into v_hjse from kpgl_dk_xxfp_qd where zbid=v_zbid_2;
            
			insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
			fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
			select v_zbid_2,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,v_mx_hjje,v_hjse,bz,
			fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
			from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
			where xsd.id=v_id;
            
            -- 差税额和金额
                        
			-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
			insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
			values (v_zbid_2,'(详见销货清单)',now(),v_fplx,'1','0','1');
            
            -- 每向待开表插入一条数据，需向关系表插入一条关联信息
			insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid_2,v_id,now());
                    
			-- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
			if v_fplx = '0' then 
                    
				insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'5000','4',now(); 
                        
			else
                    
				insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'50001','4',now(); 
                    
			end if;
            
            -- 存入负单和待开ID间的关系
			if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
				insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
				select v_zbid_2,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                    
				-- 在负单关系存入后，将其状态改为3
				update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
			end if; 
		
		end if;
        
	else
    
		if (select count(*) from kpgl_dk_xxfp where id=v_zbid_2) = 0 and (select count(*) from kpgl_dk_xxfp_mx  where zbid=v_zbid_2) > 0 then
        
			select sum(je) into v_mx_hjje from kpgl_dk_xxfp_mx where zbid=v_zbid_2;
            
            -- 由于djgl_xsdxx不传入税率，向kpgl_dk_xxfp插入税额时需从kpgl_dk_xxfp_mx表中获取
			select sum(se) into v_hjse from kpgl_dk_xxfp_mx where zbid=v_zbid_2;
            
			insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
			fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,bbh,kjrq,sjly)
			select v_zbid_2,jsp.kpfwqh,jsp.jspbh,kpdh,fplx,xsd.djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,v_mx_hjje,v_hjse,bz,
			fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,jgbm,xsd.bbh,now(),'0' 
			from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
			where xsd.id=v_id;
            
            -- 每向待开表插入一条数据，需向关系表插入一条关联信息
			insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_zbid_2,v_id,now());
                    
			-- 每向待开表插入一条信息，就需向审核表插入一条对应的信息
			if v_fplx = '0' then 
                    
				insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'5000','4',now(); 
                        
			else
                    
				insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_zbid_2,'50001','4',now(); 
                    
			end if;
            
            -- 存入负单和待开ID间的关系
			if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
				insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
				select v_zbid_2,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                    
				-- 在负单关系存入后，将其状态改为3
				update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
			end if; 
        
        end if;
        
    end if;
    
    if v_dkzt='0' then
		-- 修改销售单状态
		update djgl_xsdxx set xsdzt= 6 where id =v_id and xsdzt= 0;
	end if;
    
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_mx_cf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	 '销售单—正单处理—明细拆分: 成功',sysdate(),concat('销售单ID：',v_id,'清单标志：',v_qdbz,'销售单限额',v_dzxezp,'发票类型',v_fplx,'业务系统：',v_ywxt),null);
    
    commit;
    
 END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_mxcl
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_mxcl`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_mxcl`(in v_zbid varchar(100),in v_ywxt varchar(20))
BEGIN
		-- 用于异常排查
		declare beginTime datetime default now();
		declare error_code varchar(100);
		declare error_msg text;
		declare error_xx varchar(4000);
		declare mc_zp_cursor_flag int default 0;
        
        -- 用于将明细信息进行分组
		declare v_flbm varchar(19);
		declare v_spmc varchar(200);
		declare v_ggxh varchar(100);
		declare v_slv decimal(10,6);
		declare v_jldw varchar(100);
		declare v_hsbz varchar(1);
		declare v_fphxz varchar(1);
        
        declare v_spbh varchar(20);
		declare v_spsm varchar(100);
		declare v_xsyh varchar(1);
		declare v_yhsm varchar(100);
		declare v_lslvbs varchar(1);
 
		-- 做明细合并后的多条的明细系列号（可用于“清单”类型的条数比较）
		declare v_mxxh varchar(100);
 
		-- 清单标志所需计数器
		declare v_ts int(5);
 
		-- 负单明细金额
		declare v_fd_je decimal(16,2);
        declare v_fd_hjje decimal(16,2);
        
        declare v_sl decimal(36,15);
        declare v_je decimal(16,2);
        declare v_dj decimal(36,15);
 
		-- 筛选条件，用group by分组，存入游标xsd_mc_zp_cursor
		declare mc_zp_cursor cursor for
		select tmp_flbm,tmp_spmc,tmp_ggxh,tmp_slv,tmp_jldw,tmp_hsbz,tmp_fphxz,
        tmp_spbh,tmp_spsm,tmp_xsyh,tmp_yhsm,tmp_lslvbs from tmp_mc_zdcl_zp
		where tmp_se<>0 and tmp_je>0
		group by tmp_flbm,tmp_spmc,tmp_ggxh,tmp_slv,tmp_jldw,tmp_hsbz,tmp_fphxz,
        tmp_spbh,tmp_spsm,tmp_xsyh,tmp_yhsm,tmp_lslvbs;
 
		-- 遇到sqlexception,sqlwarning错误立即退出
		declare exit handler for sqlexception,sqlwarning
		begin
			rollback;
            GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
			set error_xx=concat('pro_mxcl','错误代码：',error_code,'错误信息：',error_msg);
			insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
			values('pro_mxcl','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单—正单合并—明细处理: ',error_xx),
			sysdate(),concat('销售单ID：',v_zbid,'业务系统：',v_ywxt),null);
			commit;
		end;
 
		-- 当游标到最后一条时，终止循环
		declare continue handler for not found set mc_zp_cursor_flag =1;
		set autocommit=0;
        
        set v_mxxh=1;
        
        delete from tmp_mx_table1;
        
        START TRANSACTION;
 
        -- 打开游标mc_zp_cursor，从tmp_xsd_zdcl_zp_mc查找信息存入tmp_xsd_mc_sx
		open mc_zp_cursor;
		loop_mc_zp:loop
			fetch mc_zp_cursor
			into v_flbm,v_spmc,v_ggxh,v_slv,v_jldw,v_hsbz,v_fphxz,
            v_spbh,v_spsm,v_xsyh,v_yhsm,v_lslvbs;
 
			if mc_zp_cursor_flag=1 then
				leave loop_mc_zp;
			end if;
 
			-- 清空tmp_mc_zdcl_zp_sx表内容
			delete from tmp_mc_zdcl_zp_sx;
 
			insert into tmp_mc_zdcl_zp_sx(tmp_fpzl,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,
			tmp_jldw,tmp_sl,tmp_dj,tmp_hsjgbz,tmp_lrrq,tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,
			tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
			select tmp_fpzl,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,
			tmp_jldw,tmp_sl,tmp_dj,tmp_hsjgbz,tmp_lrrq,tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,
			tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mc_zdcl_zp
			where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
			and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
			and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
            and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
            and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
            and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
            and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end)
            and tmp_se<>0 and tmp_je>0;				
				
				if exists(select * from tmp_xsd_zd_zpdx) then
					
					-- 销售单正单处理-专票抵消
					call pro_xsd_zddx(v_zbid,v_ywxt,v_flbm,v_spmc,v_ggxh,v_slv,v_jldw,v_hsbz,v_fphxz,v_spbh,v_spsm,v_xsyh,v_yhsm,v_lslvbs);
                    
                    if exists(select * from tmp_xsd_zd_zpdx 
						where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
						and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
						and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
						and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
						and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
						and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
						and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1) then
                    
						select sum(tmp_fd_hjje) into v_fd_je from tmp_xsd_zd_zpdx 
						where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
						and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
						and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
						and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
						and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
						and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
						and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1;
                        
					else
                    
						set v_fd_je=0;
                    
                    end if;                    
 
				else
 
					set v_fd_je=0;
 
				end if;
 
				if (select sum(tmp_je) from tmp_mc_zdcl_zp_sx) + v_fd_je <> 0 then
                                    
						select sum(tmp_je) into v_je from tmp_mc_zdcl_zp_sx;                        
						
                        if (select count(*) from tmp_mc_zdcl_zp_sx where tmp_sl is NULL) > 0 then
                    
							insert into tmp_mx_table1(tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,
							tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
							select tmp_fpzl,v_mxxh,sum(tmp_je)+v_fd_je,tmp_slv,(sum(tmp_je)+v_fd_je)*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,sum(tmp_sl),NULL,
							now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
							tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mc_zdcl_zp_sx limit 1;
                            
						else
							
                            insert into tmp_mx_table1(tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,
							tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
							select tmp_fpzl,v_mxxh,sum(tmp_je)+v_fd_je,tmp_slv,(sum(tmp_je)+v_fd_je)*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,sum(tmp_sl),cast(sum(tmp_je)/sum(tmp_sl) as decimal(36,15)),
							now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
							tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mc_zdcl_zp_sx limit 1;
                        
                        end if;

						-- 自增fpmxxh，并修改数值
						set v_mxxh=v_mxxh+1;	
                
                else
					-- 当负担的明细跟正单的明细金额抵消为零时，不插入本条数据
					set v_fd_je=0;
                    
				end if;            
			
		end loop;
 
		-- 关闭游标mc_zp_cursor
		close mc_zp_cursor;

	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_mxcl','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	 '销售单—正单合并—明细处理: 成功',sysdate(),concat('销售单ID：',v_zbid,'业务系统：',v_ywxt),null);
        
	commit;
END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_mxcl_2
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_mxcl_2`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_mxcl_2`(in v_zbid varchar(100),in v_dk_xsdid varchar(100),in v_ywxt varchar(20),
in cs_qdbz varchar(1),in cs_fplx varchar(2))
BEGIN
		-- 用于异常排查
		declare beginTime datetime default now();
		declare error_code varchar(100);
		declare error_msg text;
		declare error_xx varchar(4000);
		declare mc_zp_cursor_flag int default 0;
        
        declare v_mxid varchar(100);
        -- 用于将明细信息进行分组
		declare v_flbm varchar(19);
		declare v_spmc varchar(200);
		declare v_ggxh varchar(100);
		declare v_slv decimal(10,6);
		declare v_jldw varchar(100);
		declare v_hsbz varchar(1);
		declare v_fphxz varchar(1);
        declare v_spbh varchar(20);
		declare v_spsm varchar(100);
		declare v_xsyh varchar(1);
		declare v_yhsm varchar(100);
		declare v_lslvbs varchar(1);
 
		-- 做明细合并后的多条的明细系列号（可用于“清单”类型的条数比较）
		declare v_mxxh varchar(100);
 
		-- 负单明细金额
		declare v_fd_je decimal(16,2);
        declare v_fd_hjje decimal(16,2);
        
        declare v_sl decimal(36,15);
        declare v_je decimal(16,2);
        declare v_dj decimal(36,15);
 
		-- 筛选条件，用group by分组，存入游标xsd_mc_zp_cursor
		declare mc_zp_cursor cursor for
		select id,flbm,spmc,ggxh,slv,jldw,hsbz,fphxz,spbh,spsm,xsyh,yhsm,lslvbs from djgl_xsdxx_mx
		where xsdjid=v_zbid order by mxxh;
 
		-- 遇到sqlexception,sqlwarning错误立即退出
		declare exit handler for sqlexception,sqlwarning
		begin
			rollback;
			GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
			set error_xx=concat('pro_mxcl_2','错误代码：',error_code,'错误信息：',error_msg);
			insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
			values('pro_mxcl_2','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单—正单合并—明细（待开）处理: ',error_xx),
			sysdate(),concat('当前传入待开的销售单原ID：',v_zbid,'，待开ID：',v_dk_xsdid), 
			concat('清单标志：',cs_qdbz,'，发票类型：',cs_fplx,'，业务系统：',v_ywxt));
			commit;
		end;
 
		-- 当游标到最后一条时，终止循环
		declare continue handler for not found set mc_zp_cursor_flag =1;
		set autocommit=0;
        
        START TRANSACTION;
 
        -- 打开游标mc_zp_cursor，从tmp_xsd_zdcl_zp_mc查找信息存入tmp_xsd_mc_sx
		open mc_zp_cursor;
		loop_mc_zp:loop
			fetch mc_zp_cursor
			into v_mxid,v_flbm,v_spmc,v_ggxh,v_slv,v_jldw,v_hsbz,v_fphxz,
            v_spbh,v_spsm,v_xsyh,v_yhsm,v_lslvbs;
 
			if mc_zp_cursor_flag=1 then
				leave loop_mc_zp;
			end if;
			
			if exists(select * from tmp_xsd_zd_zpdx) then
					
				-- 销售单正单处理-专票抵消
				call pro_xsd_zddx(v_zbid,v_ywxt,v_flbm,v_spmc,v_ggxh,v_slv,v_jldw,v_hsbz,v_fphxz,v_spbh,v_spsm,v_xsyh,v_yhsm,v_lslvbs);
                    
				if exists(select * from tmp_xsd_zd_zpdx 
					where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
					and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
					and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
					and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
					and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
					and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
					and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1) then
                    
					select sum(tmp_fd_hjje) into v_fd_je from tmp_xsd_zd_zpdx 
					where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
					and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
					and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
					and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
					and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
					and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
					and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1;
                      
					update tmp_xsd_zd_zpdx set tmp_dxzt = '4' where tmp_flbm=v_flbm and tmp_spmc=v_spmc and (case when v_ggxh is null then tmp_ggxh is null else tmp_ggxh = v_ggxh end)
					and tmp_slv=v_slv and (case when v_jldw is null then tmp_jldw is null else tmp_jldw = v_jldw end)
					and tmp_hsbz=v_hsbz and tmp_fphxz=v_fphxz and (case when v_spbh is null then tmp_spbh is null else tmp_spbh = v_spbh end)
					and (case when v_spsm is null then tmp_spsm is null else tmp_spsm = v_spsm end)
					and (case when v_xsyh is null then tmp_xsyh is null else tmp_xsyh = v_xsyh end)
					and (case when v_yhsm is null then tmp_yhsm is null else tmp_yhsm = v_yhsm end)
					and (case when v_lslvbs is null then tmp_lslvbs is null else tmp_lslvbs = v_lslvbs end) and tmp_dxzt= 1;
                        
				else
                    
					set v_fd_je=0;
                    
				end if;                    
 
			else
 
				set v_fd_je=0;
 
			end if;
 
			if (select je from djgl_xsdxx_mx where id=v_mxid) + v_fd_je <> 0 then
                                    
				-- select sum(tmp_je) into v_je from tmp_mc_zdcl_zp_sx;                        
						
				-- if (select count(*) from djgl_xsdxx_mx where sl is NULL) > 0 then
                    
					-- insert into kpgl_dk_xxfp_mx(tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,
					-- tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
					-- select tmp_fpzl,v_mxxh,sum(tmp_je)+v_fd_je,tmp_slv,(sum(tmp_je)+v_fd_je)*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,sum(tmp_sl),NULL,
					-- now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
					-- tmp_yhsm,tmp_lslvbs,tmp_kce from djgl_xsdxx_mx;
                            
				-- else
							
					-- insert into kpgl_dk_xxfp_mx(tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,
					-- tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
					-- select tmp_fpzl,v_mxxh,sum(tmp_je)+v_fd_je,tmp_slv,(sum(tmp_je)+v_fd_je)*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,sum(tmp_sl),cast(sum(tmp_je)/sum(tmp_sl) as decimal(36,15)),
					-- now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
					-- tmp_yhsm,tmp_lslvbs,tmp_kce from djgl_xsdxx_mx;
                    
                    if cs_qdbz='0' then
            
						insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_dk_xsdid,cs_fplx,mxxh,je+v_fd_je,slv,(je+v_fd_je)*slv,spbh,spmc,spsm,ggxh,jldw,sl,cast((je+v_fd_je)/sl as decimal(36,15)),now(),lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;
            
					end if;
		
					if cs_qdbz='1' then
            
						insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_dk_xsdid,cs_fplx,mxxh,je+v_fd_je,slv,(je+v_fd_je)*slv,spbh,spmc,spsm,ggxh,jldw,sl,cast((je+v_fd_je)/sl as decimal(36,15)),now(),lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where id=v_mxid;
            
					end if;
                        
				-- end if;	
                    
			end if;
			
		end loop;
		-- 关闭游标mc_zp_cursor
		close mc_zp_cursor;

	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_mxcl_2','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单—正单合并—明细（待开）处理: ',sysdate(),concat('当前传入待开的销售单原ID：',v_zbid,'，待开ID：',v_dk_xsdid), 
	concat('清单标志：',cs_qdbz,'，发票类型：',cs_fplx,'，业务系统：',v_ywxt));
        
	commit;
END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_gx
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_gx`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_gx`(in v_dk_xsdid varchar(100))
BEGIN

	declare gx_cursor_flag int default 0;
    
    declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
    
    declare v_id varchar(100);
    
    declare v_sl bigint(100);

	declare gx_cursor cursor for 
    select tmp_id from tmp_xsd_sx;
    
    -- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_gx','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_gx','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('向关系表存入信息: ',error_xx),
		sysdate(),concat('传入参数v_dk_xsdid：',v_dk_xsdid), concat('存入失败的待开ID',v_dk_xsdid));
		commit;
	end;
    
    set v_sl = 0;
    
    open gx_cursor;
    loop_gx:loop
		fetch gx_cursor into v_id;
        
		if gx_cursor_flag=1 then
			leave loop_gx;
		end if;
        
        insert into djgl_dkfpgx(dkid,xsdid,lrrq) values(v_dk_xsdid,v_id,now());
        
        set v_sl = v_sl + 1 ;
        
	end loop;
    close gx_cursor;
    
    insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_gx','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'向关系表存入信息: 成功',sysdate(),concat('传入参数v_dk_xsdid：',v_dk_xsdid), concat('存入数量：',v_sl));
    
    commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsdcl_2
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsdcl_2`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsdcl_2`(in v_id varchar(100),in v_jelj decimal(16,2),in v_ts int(5),in cs_dzxezp decimal(16,2),
in cs_qdbz varchar(1),in cs_fplx varchar(2),in v_ywxt varchar(20))
BEGIN
	-- 用于异常排查
	declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
	-- 用于存取插入表的ID
	declare v_dk_xsdid varchar(100);
	-- 存取所有可以抵消的负单的和
	declare v_fd_hjje decimal(16,2);    
    -- 用于统计税率
	declare v_hjse decimal(16,2);
    -- 存取待开ID
    declare v_dkid varchar(100);
    -- 用于存取备注信息
    declare v_bz varchar(500);
    
	-- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_xsdcl_2','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_xsdcl_2','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理-判断当前合并内容是否超限: ',error_xx),
		sysdate(),concat('当前传入合并的销售单ID：',v_id,'，当前累计金额：',v_jelj,'，当前累计明细数量：',v_ts,'，限额：',cs_dzxezp), 
        concat('清单标志：',cs_qdbz,'，发票类型：',cs_fplx,'，业务系统：',v_ywxt));
		commit;
	end;
    
    if(select count(*) from tmp_mx_table1) <= v_ts and v_jelj <= cs_dzxezp then 
    
		delete from tmp_mx_table2;
        
		insert into tmp_mx_table2(tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,
		tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
		select tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,
		tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mx_table1;
        
		delete from tmp_mc_zdcl_zp;
        
		insert into tmp_mc_zdcl_zp(tmp_fpzl,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,
		tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,
		tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
		select tmp_fpzl,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,
		tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,
		tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mx_table2; 
            
		insert into tmp_xsd_sx (tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
		tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
		tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq)
		select tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
		tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
		tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq
		from tmp_xsd_zdcl_zp where tmp_id=v_id;
            
	else
        
		-- 获取所有可以抵消的负单金额的和
		SELECT SUM(tmp_fd_hjje) INTO v_fd_hjje FROM tmp_xsd_zd_zpdx WHERE tmp_dxzt = 1;
    
		if v_fd_hjje is null then
    
			set v_fd_hjje=0;
    
		end if;
            
		if (select sum(tmp_hjje) from tmp_xsd_sx) + v_fd_hjje <> 0 then
        
			select concat('XSD-',uuid()) into v_dk_xsdid;
            
			-- 大于一条的话，为自动合并
			if (select count(*) from tmp_xsd_sx) > 1 then 
    
				if cs_qdbz='0' then
            
					insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
					lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
					select v_dk_xsdid,tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_je*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,
					now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
					tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mx_table2;
            
				end if;
            
				if cs_qdbz='1' then
            
					insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
					lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
					select v_dk_xsdid,tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_je*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,
					now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
					tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mx_table2;
                    
					-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
					insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
					values (v_dk_xsdid,'(详见销货清单)',now(),cs_fplx,'1','0','1');
            
				end if;
                
				-- 获取清单标志，判断可输入明细条数（6000 or 8）
				if cs_qdbz='0' then
					select sum(se) into v_hjse from kpgl_dk_xxfp_mx where zbid=v_dk_xsdid;
				else
					select sum(se) into v_hjse from kpgl_dk_xxfp_qd where zbid=v_dk_xsdid;
				end if;
                
				update djgl_xsdxx set xsdzt = 5 where id in (select tmp_id from tmp_xsd_sx) and xsdzt='0';

				-- 获取备注，合并后不超过200个字符
				select left(REPLACE(GROUP_CONCAT(tmp_bz),',',''),200) into v_bz from tmp_xsd_sx;
	
				-- 插入销售单信息,执行合并
				insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
				fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,kjrq,bbh,sjly)
				select v_dk_xsdid,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,CONCAT('DJBH_',LEFT(REPLACE(DATE_FORMAT(current_timestamp(3),'%Y%m%d%T%f'),':',''),17)),tmp_gfmc,tmp_gfsh,
				tmp_gfdzdh,tmp_gfyhzh,tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,sum(tmp_hjje)+v_fd_hjje,v_hjse,v_bz,
				tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,
				tmp_by2,CONCAT('LSH_',LEFT(REPLACE(DATE_FORMAT(current_timestamp(3),'%Y%m%d%T%f'),':',''),17)),tmp_ywlx,tmp_ywxt,tmp_jgbm,now(),tmp_bbh,'0' from tmp_xsd_sx limit 1;
                
                -- select v_id;
                
                delete from tmp_xsd_sx;
                
                insert into tmp_xsd_sx(tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
				tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
				tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq)
				select tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
				tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
				tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq
				from tmp_xsd_zdcl_zp where tmp_id=v_id;
        
				delete from tmp_mc_zdcl_zp;
            
				insert into tmp_mc_zdcl_zp(tmp_fpzl,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,
				tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,
				tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
				select cs_fplx,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,lryhid,by1,by2,fphxz,hsbz,
				flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where xsdjid=v_id;
            
				call pro_mxcl(v_id,v_ywxt);
                
				-- 每向待开表插入一条数据，需向关系表插入一条关联信息
				call pro_gx(v_dk_xsdid);
                
				-- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
				if cs_fplx = '0' then 
					insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'5000','4',now();                         
				else                    
					insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'50001','4',now(); 						
				end if;
                
                -- 存入负单和待开ID间的关系
				if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '1') > 0 then 
                    
					insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
					select v_dk_xsdid,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '1';
                    
                    -- 在负单关系存入后，将其状态改为3
					update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '1';
                    
				end if;
                
			end if;
        
			-- 等于一条的话，为自动待开
			if (select count(*) from tmp_xsd_sx) = 1 then 
    
				-- if cs_qdbz='0' then
            
				-- 	insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
				-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
				-- 	select v_dk_xsdid,cs_fplx,mxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,cast(je/sl as decimal(36,15)),now(),lrryid,lrrymc,
				-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where xsdjid in (select tmp_id from tmp_xsd_sx);
            
				-- end if;
		
				-- if cs_qdbz='1' then
            
				-- 	insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
				-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
				-- 	select v_dk_xsdid,cs_fplx,mxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,cast(je/sl as decimal(36,15)),now(),lrryid,lrrymc,
				-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where xsdjid in (select tmp_id from tmp_xsd_sx);
            
					-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
				-- 	insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
				-- 	values (v_dk_xsdid,'(详见销货清单)',now(),cs_fplx,'1','0','1');
            
				-- end if;
                
                select tmp_id into v_dkid from tmp_xsd_sx;
                
                call pro_mxcl_2(v_dkid,v_dk_xsdid,v_ywxt,cs_qdbz,cs_fplx);
                
				-- 获取清单标志，判断可输入明细条数（6000 or 8）
				if cs_qdbz='0' then
					select sum(se) into v_hjse from kpgl_dk_xxfp_mx where zbid=v_dk_xsdid;
				else
					select sum(se) into v_hjse from kpgl_dk_xxfp_qd where zbid=v_dk_xsdid;
				end if;
                
				update djgl_xsdxx set xsdzt = 6 where id in (select tmp_id from tmp_xsd_sx) and xsdzt='0';
        
				-- 插入销售单信息,执行合并
				insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
				fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,kjrq,bbh,sjly)
				select v_dk_xsdid,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,
				tmp_gfdzdh,tmp_gfyhzh,tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,sum(tmp_hjje)+v_fd_hjje,v_hjse,left(REPLACE(GROUP_CONCAT(tmp_bz),',',''),200),
				tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,
				tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,now(),tmp_bbh,'0' from tmp_xsd_sx limit 1;
                
                delete from tmp_xsd_sx;
                
                insert into tmp_xsd_sx(tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
				tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
				tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq)
				select tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
				tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
				tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq
				from tmp_xsd_zdcl_zp where tmp_id=v_id;
        
				delete from tmp_mc_zdcl_zp;
            
				insert into tmp_mc_zdcl_zp(tmp_fpzl,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,
				tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,
				tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
				select cs_fplx,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,lryhid,by1,by2,fphxz,hsbz,
				flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where xsdjid=v_id;
            
				call pro_mxcl(v_id,v_ywxt);
                
				-- 每向待开表插入一条数据，需向关系表插入一条关联信息
				call pro_gx(v_dk_xsdid);
                
				-- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
				if cs_fplx = '0' then 
					insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'5000','4',now();                         
				else                    
					insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'50001','4',now(); 						
				end if;
                
                -- 存入负单和待开ID间的关系
				if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
					insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
					select v_dk_xsdid,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                    
                    -- 在负单关系存入后，将其状态改为3
					update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
				end if;
        
			end if;
		
        else
        
			update djgl_xsdxx set xsdzt = 8 where id in (select tmp_id from tmp_xsd_sx) and xsdzt='0';
            
		end if;
        
	end if;
    
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsdcl_2','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单正单处理-判断当前合并内容是否超限:  录入成功',sysdate(),concat('当前传入合并的销售单ID：',v_id,'，当前累计金额：',v_jelj,'，当前累计明细数量：',v_ts,'，限额：',cs_dzxezp),
    concat('清单标志：',cs_qdbz,'，发票类型：',cs_fplx,'，业务系统：',v_ywxt));
        
	commit;    
        
END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsdcl
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsdcl`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsdcl`(in cs_dzxezp decimal(16,2),in cs_qdbz varchar(1),in cs_fplx varchar(2),
in v_ywxt varchar(20))
BEGIN

	declare xs_cursor_flag int default 0;
    
	declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);

	declare v_id varchar(100);
    declare v_hjje decimal(16,2);
    
    declare v_jelj decimal(16,2);
    
    declare v_ts int(5);
    declare v_count int(5);
    
    -- 获取开票服务器号
	declare v_kpfwqh varchar(100);
    
    -- 做明细合并后的多条的明细系列号（可用于“清单”类型的条数比较）
	declare v_mxxh varchar(100);
    
	-- 用于存取插入表的ID
	declare v_dk_xsdid varchar(100);
    
    -- 存取所有可以抵消的负单的和
	declare v_fd_hjje decimal(16,2);
    
    -- 用于统计税率
	declare v_hjse decimal(16,2);
    
    -- 存取备注
	declare v_bz varchar(500);

	-- 存取待开ID
    declare v_dkid varchar(100);

	declare sx_cursor cursor for
	select tmp_id,tmp_hjje
	from tmp_xsd_zdcl_zp order by tmp_hjje;
    
    declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_xsdcl','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_xsdcl','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单—正单合并—销售单处理1: ',error_xx),
		sysdate(),concat('最大限额：',cs_dzxezp,'清单标志：',cs_qdbz,'发票类型：',cs_fplx,'业务系统：',v_ywxt),null);
		commit;
	end;
    
    declare continue handler for not found set xs_cursor_flag =1;
	set autocommit=0;
    
    if cs_qdbz='0' then    
		set v_ts=8;
    else
		set v_ts=6000;
    end if;
    
    set v_jelj=0;

	open sx_cursor;
	loop_sx:loop
		fetch sx_cursor
		into v_id,v_hjje;
        
        if xs_cursor_flag=1 then
        
			-- 获取所有可以抵消的负单金额的和
			SELECT SUM(tmp_fd_hjje) INTO v_fd_hjje FROM tmp_xsd_zd_zpdx WHERE tmp_dxzt = 1;
    
			if v_fd_hjje is null then
    
				set v_fd_hjje=0;
    
			end if;
            
            if (select sum(tmp_hjje) from tmp_xsd_sx) + v_fd_hjje <> 0 then
			
				select concat('XSD-',uuid()) into v_dk_xsdid;
            
				-- 大于一条的话，为自动合并
				if (select count(*) from tmp_xsd_sx) > 1 then 
    
					if cs_qdbz='0' then
            
						insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_dk_xsdid,tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_je*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,
						now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
						tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mx_table2;
            
					end if;
            
					if cs_qdbz='1' then
            
						insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
						lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
						select v_dk_xsdid,tmp_fpzl,tmp_mxxh,tmp_je,tmp_slv,tmp_je*tmp_slv,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,tmp_jldw,tmp_sl,tmp_dj,
						now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,tmp_flbm,tmp_xsyh,
						tmp_yhsm,tmp_lslvbs,tmp_kce from tmp_mx_table2;
                    
						-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
						insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
						values (v_dk_xsdid,'(详见销货清单)',now(),cs_fplx,'1','0','1');
            
					end if;
                
					-- select v_id,v_jelj,v_ts,cs_dzxezp;
                
					-- 获取清单标志，判断可输入明细条数（6000 or 8）
					if cs_qdbz='0' then
						select sum(se) into v_hjse from kpgl_dk_xxfp_mx where zbid=v_dk_xsdid;
					else
						select sum(se) into v_hjse from kpgl_dk_xxfp_qd where zbid=v_dk_xsdid;
					end if;
                
					update djgl_xsdxx set xsdzt = 5 where id in (select tmp_id from tmp_xsd_sx) and xsdzt='0';

					-- 获取备注，合并后不超过200个字符
					select left(REPLACE(GROUP_CONCAT(tmp_bz),',',''),200) into v_bz from tmp_xsd_sx;
        
					-- 插入销售单信息,执行合并
					insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,kjrq,bbh,sjly)
					select v_dk_xsdid,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,CONCAT('DJBH_',LEFT(REPLACE(DATE_FORMAT(current_timestamp(3),'%Y%m%d%T%f'),':',''),17)),tmp_gfmc,tmp_gfsh,
					tmp_gfdzdh,tmp_gfyhzh,tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,sum(tmp_hjje)+v_fd_hjje,v_hjse,v_bz,
					tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,
					tmp_by2,CONCAT('LSH_',LEFT(REPLACE(DATE_FORMAT(current_timestamp(3),'%Y%m%d%T%f'),':',''),17)),tmp_ywlx,tmp_ywxt,tmp_jgbm,now(),tmp_bbh,'0' from tmp_xsd_sx limit 1;
    
					-- 每向待开表插入一条数据，需向关系表插入一条关联信息
					call pro_gx(v_dk_xsdid);
                
					-- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
					if cs_fplx = '0' then 
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'5000','4',now(); 
					else
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'50001','4',now(); 
					end if;
                    
                    -- 存入负单和待开ID间的关系
                    if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '1') > 0 then 
                    
						insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
                        select v_dk_xsdid,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '1';
                    
						-- 在负单关系存入后，将其状态改为3
                        update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '1';
                    
                    end if;
    
				end if;
        
				-- 等于一条的话，为自动待开
				if (select count(*) from tmp_xsd_sx) = 1 then 
    
					-- if cs_qdbz='0' then
            
					-- 	insert into kpgl_dk_xxfp_mx(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
					-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
					-- 	select v_dk_xsdid,cs_fplx,mxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,cast(je/sl as decimal(36,15)),now(),lrryid,lrrymc,
					-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where xsdjid in (select tmp_id from tmp_xsd_sx);
            
					-- end if;
            
					-- if cs_qdbz='1' then
            
					-- 	insert into kpgl_dk_xxfp_qd(zbid,fpzl,fpmxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,lrrq,lrryid,lrrymc,
					-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce)
					-- 	select v_dk_xsdid,cs_fplx,mxxh,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,cast(je/sl as decimal(36,15)),now(),lrryid,lrrymc,
					-- 	lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx where xsdjid in (select tmp_id from tmp_xsd_sx);
            
						-- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
					-- 	insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
					-- 	values (v_dk_xsdid,'(详见销货清单)',now(),cs_fplx,'1','0','1');
            
					-- end if;
                    
					select tmp_id into v_dkid from tmp_xsd_sx;
                
					call pro_mxcl_2(v_dkid,v_dk_xsdid,v_ywxt,cs_qdbz,cs_fplx);
                
					-- 获取清单标志，判断可输入明细条数（6000 or 8）
					if cs_qdbz='0' then
                    
						select sum(se) into v_hjse from kpgl_dk_xxfp_mx where zbid=v_dk_xsdid;
                        
					else
                    
						select sum(se) into v_hjse from kpgl_dk_xxfp_qd where zbid=v_dk_xsdid;
                        
                        -- 在kpgl_dk_xxfp_mx表插入(x详见销货清单)条
						insert into kpgl_dk_xxfp_mx(zbid,spmc,lrrq,fpzl,fphxz,hsbz,fpmxxh) 
						values (v_dk_xsdid,'(详见销货清单)',now(),cs_fplx,'1','0','1');
                        
					end if;
                
					update djgl_xsdxx set xsdzt = 6 where id in (select tmp_id from tmp_xsd_sx) and xsdzt='0';
        
					-- 插入销售单信息,执行合并
					insert into kpgl_dk_xxfp(id,kpfwqh,jspbh,kpdh,fpzl,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
					fhr,skr,dybz,qdbz,lrrq,lrryid,lrrymc,lryhid,by1,by2,lsh,ywlx,ywxt,jgbm,kjrq,bbh,sjly)
					select v_dk_xsdid,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,
					tmp_gfdzdh,tmp_gfyhzh,tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,sum(tmp_hjje)+v_fd_hjje,v_hjse,left(REPLACE(GROUP_CONCAT(tmp_bz),',',''),200),
					tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,now(),tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,
					tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,now(),tmp_bbh,'0' from tmp_xsd_sx limit 1;
                
					-- 每向待开表插入一条数据，需向关系表插入一条关联信息
					call pro_gx(v_dk_xsdid);
                
					-- select v_id,v_jelj,v_ts,cs_dzxezp;
                
					-- 每向待开表插入一条信息，就需想审核表插入一条对应的信息
					if cs_fplx = '0' then 
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'5000','4',now(); 
					else
						insert into gy_shlcmx(ywid,ywlbdm,ywzt,lrrq) select v_dk_xsdid,'50001','4',now(); 
					end if;
                    
                    -- 存入负单和待开ID间的关系
                    if (select count(*) from tmp_xsd_zd_zpdx where tmp_dxzt = '4') > 0 then 
                    
						insert into djgl_dkfpgx(dkid,xsdid,lrrq) 
                        select v_dk_xsdid,tmp_id,now() from tmp_xsd_zd_zpdx where tmp_dxzt = '4';
                        
                        -- 在负单关系存入后，将其状态改为3
                        update tmp_xsd_zd_zpdx set tmp_dxzt = '3' where tmp_dxzt = '4';
                    
                    end if;
        
				end if;
                
			else
            
				update djgl_xsdxx set xsdzt = 8 where id in (select tmp_id from tmp_xsd_sx) and xsdzt='0';
            
            end if;
            
			leave loop_sx;
            
		end if;
        
        -- delete from tmp_mc_zdcl_zp;
        
        insert into tmp_mc_zdcl_zp(tmp_fpzl,tmp_je,tmp_slv,tmp_se,tmp_spbh,tmp_spmc,tmp_spsm,tmp_ggxh,
		tmp_jldw,tmp_sl,tmp_dj,tmp_lrrq,tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_fphxz,tmp_hsbz,
		tmp_flbm,tmp_xsyh,tmp_yhsm,tmp_lslvbs,tmp_kce)
		select cs_fplx,je,slv,se,spbh,spmc,spsm,ggxh,jldw,sl,dj,now(),
		lrryid,lrrymc,lryhid,by1,by2,fphxz,hsbz,flbm,xsyh,yhsm,lslvbs,kce from djgl_xsdxx_mx 
		where xsdjid=v_id;
        
        call pro_mxcl(v_id,v_ywxt);
        
        set v_jelj=v_jelj+v_hjje;
        
        call pro_xsdcl_2(v_id,v_jelj,v_ts,cs_dzxezp,cs_qdbz,cs_fplx,v_ywxt);
    
    end loop;
    close sx_cursor;
    
    insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsdcl','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单—正单合并—销售单处理1: 成功',sysdate(),concat('最大限额：',cs_dzxezp,'清单标志：',cs_qdbz,'发票类型：',cs_fplx,'业务系统：',v_ywxt),null);
    
    commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsd_tmp_table
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsd_tmp_table`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsd_tmp_table`()
BEGIN

-- 用于异常排查
declare beginTime datetime default now();
declare error_code varchar(100);
declare error_msg text;
declare error_xx varchar(4000);
declare mc_zp_cursor_flag int default 0;

-- 遇到sqlexception,sqlwarning错误立即退出
declare exit handler for sqlexception,sqlwarning
begin
	rollback;
	GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
	set error_xx=concat('pro_xsd_tmp_table','错误代码：',error_code,'错误信息：',error_msg);
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
	values('pro_xsd_tmp_table','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单—临时表创建: ',error_xx),
	sysdate(),null,null);
	commit;
end;

-- 用于存取djgl_xsdxx表的信息,tmp_xsd_zdcl_zp
drop table if exists tmp_xsd_zdcl_zp;
create temporary table tmp_xsd_zdcl_zp(
	tmp_id varchar(100), 
	tmp_kpfwqh varchar(100), 
	tmp_jspbh varchar(100),
	tmp_kpdh varchar(100),
	tmp_fpzl varchar(1),
	tmp_djbh varchar(100),
	tmp_gfmc varchar(200),
	tmp_gfsh varchar(100),
	tmp_gfdzdh varchar(200),
	tmp_gfyhzh varchar(100),
	tmp_xfmc varchar(200),
	tmp_xfsh varchar(100),
	tmp_xfdzdh varchar(200),
	tmp_xfyhzh varchar(100),
	tmp_hjje decimal(16,2),
	tmp_hjse decimal(16,2),
	tmp_bz varchar(500),
	tmp_fhr varchar(200),
	tmp_skr varchar(200),
	tmp_dybz varchar(1),
	tmp_qdbz varchar(1),
	tmp_lrrq timestamp,
	tmp_lrryid varchar(100),
	tmp_lrrymc varchar(200),
	tmp_lryhid varchar(100),
	tmp_by1 varchar(100),
	tmp_by2 varchar(100),
	tmp_lsh varchar(100),
	tmp_ywlx varchar(50),
	tmp_ywxt varchar(20),
	tmp_jgbm varchar(100),
	tmp_bbh varchar(100),
	tmp_kjrq datetime
);

drop table if exists tmp_mc_zdcl_zp_sx;
create temporary table tmp_mc_zdcl_zp_sx(
	-- tmp_zbid varchar(100),
	tmp_fpzl varchar(2),
	-- tmp_fpmxxh varchar(100),
	tmp_je decimal(16,2),
	tmp_slv decimal(10,6),
	tmp_se decimal(16,2),
	tmp_spbh varchar(20),
	tmp_spmc varchar(200),
	tmp_spsm varchar(100),
	tmp_ggxh varchar(100),
	tmp_jldw varchar(100),
	tmp_sl decimal(36,15),
	tmp_dj decimal(36,15),
	tmp_hsjgbz varchar(255),
	tmp_lrrq datetime,
	tmp_lrryid varchar(100),
	tmp_lrrymc varchar(200),
	tmp_lryhid varchar(100),
	tmp_by1 varchar(100),
	tmp_by2 varchar(100),
	tmp_fphxz varchar(1),
	tmp_hsbz varchar(1),
	tmp_flbm varchar(19),
	tmp_xsyh varchar(1),
	tmp_yhsm varchar(100),
	tmp_lslvbs varchar(1),
	tmp_zzstsgl varchar(50),
	tmp_kce decimal(24,0)
);

-- 创建临时表存入明细数据
drop table if exists tmp_mc_zdcl_zp;
create temporary table tmp_mc_zdcl_zp(
	tmp_zbid varchar(100),
    tmp_fpzl varchar(2),
    -- tmp_fpmxxh varchar(100),
    tmp_je decimal(16,2),
    tmp_slv decimal(10,6),
    tmp_se decimal(16,2),
    tmp_spbh varchar(20),
    tmp_spmc varchar(200),
    tmp_spsm varchar(100),
    tmp_ggxh varchar(100),
    tmp_jldw varchar(100),
    tmp_sl decimal(36,15),
    tmp_dj decimal(36,15),
    tmp_hsjgbz varchar(255),
    tmp_lrrq datetime,
    tmp_lrryid varchar(100),
    tmp_lrrymc varchar(200),
    tmp_lryhid varchar(100),
    tmp_by1 varchar(100),
    tmp_by2 varchar(100),
    tmp_fphxz varchar(1),
    tmp_hsbz varchar(1),
    tmp_flbm varchar(19),
    tmp_xsyh varchar(1),
    tmp_yhsm varchar(100),
    tmp_lslvbs varchar(1),
    tmp_zzstsgl varchar(50),
    tmp_kce decimal(24,0)
);

-- 正单和负单抵消的状态码：1.满足抵消条件；2.不满足抵消条件
drop table if exists tmp_xsd_zd_zpdx;
create temporary table tmp_xsd_zd_zpdx(
	tmp_id varchar(100),
	tmp_fd_hjje decimal(16,2),
	tmp_flbm varchar(19),
	tmp_spmc varchar(200),
	tmp_ggxh varchar(100),
	tmp_slv decimal(10,6),
	tmp_jldw varchar(100),
	tmp_hsbz varchar(1),
	tmp_fphxz varchar(1),
    tmp_spbh varchar(20),
    tmp_spsm varchar(100),
    tmp_xsyh varchar(1),
    tmp_yhsm varchar(100),
    tmp_lslvbs varchar(1),
	tmp_dxzt int(10) default 0 
);

drop table if exists tmp_xsd_sx;
create temporary table tmp_xsd_sx(
	tmp_id varchar(100), 
	tmp_kpfwqh varchar(100), 
	tmp_jspbh varchar(100),
	tmp_kpdh varchar(100),
	tmp_fpzl varchar(1),
	tmp_djbh varchar(100),
	tmp_gfmc varchar(200),
	tmp_gfsh varchar(100),
	tmp_gfdzdh varchar(200),
	tmp_gfyhzh varchar(100),
	tmp_xfmc varchar(200),
	tmp_xfsh varchar(100),
	tmp_xfdzdh varchar(200),
	tmp_xfyhzh varchar(100),
	tmp_hjje decimal(16,2),
	tmp_hjse decimal(16,2),
	tmp_bz varchar(500),
	tmp_fhr varchar(200),
	tmp_skr varchar(200),
	tmp_dybz varchar(1),
	tmp_qdbz varchar(1),
	tmp_lrrq timestamp,
	tmp_lrryid varchar(100),
	tmp_lrrymc varchar(200),
	tmp_lryhid varchar(100),
	tmp_by1 varchar(100),
	tmp_by2 varchar(100),
	tmp_lsh varchar(100),
	tmp_ywlx varchar(50),
	tmp_ywxt varchar(20),
	tmp_jgbm varchar(100),
	tmp_bbh varchar(100),
	tmp_kjrq datetime
);

drop table if exists tmp_mx_table1;
create temporary table tmp_mx_table1(
	-- tmp_zbid varchar(100),
	tmp_fpzl varchar(2),
	tmp_mxxh varchar(100),
	tmp_je decimal(16,2),
	tmp_slv decimal(10,6),
	tmp_se decimal(16,2),
	tmp_spbh varchar(20),
	tmp_spmc varchar(200),
	tmp_spsm varchar(100),
	tmp_ggxh varchar(100),
	tmp_jldw varchar(100),
	tmp_sl decimal(36,15),
	tmp_dj decimal(36,15),
	tmp_hsjgbz varchar(255),
	tmp_lrrq datetime,
	tmp_lrryid varchar(100),
	tmp_lrrymc varchar(200),
	tmp_lryhid varchar(100),
	tmp_by1 varchar(100),
	tmp_by2 varchar(100),
	tmp_fphxz varchar(1),
	tmp_hsbz varchar(1),
	tmp_flbm varchar(19),
	tmp_xsyh varchar(1),
	tmp_yhsm varchar(100),
	tmp_lslvbs varchar(1),
	tmp_zzstsgl varchar(50),
	tmp_kce decimal(24,0)
);

drop table if exists tmp_mx_table2;
create temporary table tmp_mx_table2(
	-- tmp_zbid varchar(100),
	tmp_fpzl varchar(2),
	tmp_mxxh varchar(100),
	tmp_je decimal(16,2),
	tmp_slv decimal(10,6),
	tmp_se decimal(16,2),
	tmp_spbh varchar(20),
	tmp_spmc varchar(200),
	tmp_spsm varchar(100),
	tmp_ggxh varchar(100),
	tmp_jldw varchar(100),
	tmp_sl decimal(36,15),
	tmp_dj decimal(36,15),
	tmp_hsjgbz varchar(255),
	tmp_lrrq datetime,
	tmp_lrryid varchar(100),
	tmp_lrrymc varchar(200),
	tmp_lryhid varchar(100),
	tmp_by1 varchar(100),
	tmp_by2 varchar(100),
	tmp_fphxz varchar(1),
	tmp_hsbz varchar(1),
	tmp_flbm varchar(19),
	tmp_xsyh varchar(1),
	tmp_yhsm varchar(100),
	tmp_lslvbs varchar(1),
	tmp_zzstsgl varchar(50),
	tmp_kce decimal(24,0)
);

insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsd_tmp_table','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
'销售单—临时表创建: 成功',sysdate(),null,null);

commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsd_zp_fdhq
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsd_zp_fdhq`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsd_zp_fdhq`(in v_xfsh varchar(100),in v_gfsh varchar(200),in v_fplx varchar(2),in v_ywxt varchar(20),
in v_ywlx varchar(50),in v_jspbh varchar(200),in v_kpdh varchar(200),in v_qdbz varchar(1),in v_jgbm varchar(100))
BEGIN
    
    declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
    
    -- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_xsd_pp_fdhq','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_xsd_zp_fdhq','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单—专票—负单获取: ',error_xx),
		sysdate(),concat('销方税号：',v_xfsh,'，购方税号：',v_gfsh,'，发票类型：',v_fplx,'，业务系统：',
		v_ywxt,'，业务类型：',v_ywlx,'，金税盘号：',v_jspbh,'，开票点号：',v_kpdh,'，清单标志：',v_qdbz,'，机构编码：',v_jgbm),null);
		commit;
	end;
    
	if (select count(*) from djgl_xsdxx xsd left join djgl_xsdxx_mx mx on (xsd.id=mx.xsdjid) 
        where xsd.hjje<0 and mx.je<0 and xfsh=v_xfsh and gfsh=v_gfsh and fplx=v_fplx and ywxt=v_ywxt and ywlx=v_ywlx and
 		xsd.jspbh=v_jspbh and kpdh=v_kpdh and qdbz=v_qdbz and jgbm=v_jgbm and xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0') > 0 then
		
        insert into tmp_xsd_zd_zpdx(tmp_id,tmp_fd_hjje,tmp_flbm,tmp_spmc,tmp_ggxh,tmp_slv,tmp_jldw,tmp_hsbz,tmp_fphxz,
        tmp_spbh,tmp_spsm,tmp_xsyh,tmp_yhsm,tmp_lslvbs)
		select xsd.id,xsd.hjje,mx.flbm,mx.spmc,mx.ggxh,mx.slv,mx.jldw,mx.hsbz,mx.fphxz,mx.spbh,mx.spsm,mx.xsyh,mx.yhsm,mx.lslvbs  
        from djgl_xsdxx xsd left join djgl_xsdxx_mx mx on (xsd.id=mx.xsdjid) 
        where xsd.hjje<0 and mx.je<0 and xfsh=v_xfsh and gfsh=v_gfsh and fplx=v_fplx and ywxt=v_ywxt and ywlx=v_ywlx and
 		xsd.jspbh=v_jspbh and kpdh=v_kpdh and qdbz=v_qdbz and jgbm=v_jgbm and xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0';
    
    end if;
    
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsd_zp_fdhq','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单—专票—负单获取: 成功',sysdate(),concat('销方税号：',v_xfsh,'，购方税号：',v_gfsh,'，发票类型：',v_fplx,'，业务系统：',
    v_ywxt,'，业务类型：',v_ywlx,'，金税盘号：',v_jspbh,'，开票点号：',v_kpdh,'，清单标志：',v_qdbz,'，机构编码：',v_jgbm),null);
    
    commit;
    
END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsd_zdcl_zp
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsd_zdcl_zp`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsd_zdcl_zp`(cs_xfsh varchar(100),cs_fplx varchar(2))
BEGIN
declare xsdxx_zp_cursor_flag int default 0;
declare beginTime datetime default now();
declare error_code varchar(100);
declare error_msg text;
declare error_xx varchar(4000);
-- 用于信息筛选，存于游标xsdxx_zp_cursor
declare v_xfsh varchar(100);
declare v_gfsh varchar(100);
declare v_fplx varchar(2);
declare v_ywxt varchar(20);
declare v_ywlx varchar(50);
declare v_jspbh varchar(200);
declare v_kpdh varchar(200);
declare v_qdbz varchar(1);
declare v_jgbm varchar(100);

-- 获取开票服务器号
declare v_kpfwqh varchar(100);
declare v_sum_hjje decimal(16,2);

-- 用于存取插入表的ID
declare v_dk_xsdid varchar(100);

-- 存取所有可以抵消的负单的和
declare v_fd_hjje decimal(16,2);
 
-- 插入（详见销货清单）所需字段
declare v_qd_se decimal(16,2);
declare v_qd_je decimal(16,2);

-- 设置执行次数
declare v_1 varchar(100);

-- 用于统计税率
declare v_hjse decimal(16,2);

-- 存取限额
declare v_dzxezp decimal(16,2);

declare xsdxx_zp_cursor cursor for
select xsd.xfsh,xsd.gfsh,xsd.fplx,xsd.ywxt,xsd.ywlx,xsd.jspbh,xsd.kpdh,xsd.qdbz,xsd.jgbm
from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh)
where xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx and xsd.xsdzt='0' and xsd.hjje>0 and xsd.zfbz='0' and xsd.dkbz='0' and xsd.hjje<=jsp.dzxezp
group by xsd.xfsh,xsd.gfsh,xsd.fplx,xsd.ywxt,xsd.ywlx,xsd.jspbh,xsd.kpdh,xsd.qdbz,xsd.jgbm;

-- 遇到sqlexception,sqlwarning错误立即退出
declare exit handler for sqlexception,sqlwarning
begin
	rollback;
	GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
	set error_xx=concat('pro_xsd_zdcl_zp','错误代码：',error_code,'错误信息：',error_msg);
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
	values('pro_xsd_zdcl_zp','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理专票合并信息: ',error_xx),
		sysdate(),concat('传入参数cs_xfsh：',cs_xfsh), concat('传入参数cs_fplx：',cs_fplx));
	commit;
end;

-- 当游标循环遍历结束，结束游标
declare continue handler for not found set xsdxx_zp_cursor_flag =1;
set autocommit=0;

START TRANSACTION;

-- 搭建临时表
call pro_xsd_tmp_table();

set v_1 = 0;

open xsdxx_zp_cursor;
loop_xsdxx_zp:loop
	fetch xsdxx_zp_cursor
	into v_xfsh,v_gfsh,v_fplx,v_ywxt,v_ywlx,v_jspbh,v_kpdh,v_qdbz,v_jgbm;
 
	if xsdxx_zp_cursor_flag=1 then
 		leave loop_xsdxx_zp;
 	end if;
    
    set v_1 = v_1 + 1;
 
	-- 清空tmp_xsd_zdcl_zp表内容
	delete from tmp_xsd_zdcl_zp;
 
	-- 插入信息
	if exists(select * from jkgl_jspxx where nsrsbh=v_xfsh and jspbh=v_jspbh) then
		select kpfwqh into v_kpfwqh from jkgl_jspxx where nsrsbh=v_xfsh and jspbh=v_jspbh;
        select dzxezp into v_dzxezp from jkgl_jspxx where nsrsbh=v_xfsh and jspbh=v_jspbh;
	else    
		set v_kpfwqh=NULL;
	end if;
    
	insert into tmp_xsd_zdcl_zp(tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
	tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
	tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq)
	select xsd.id,v_kpfwqh,xsd.jspbh,kpdh,fplx,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
	fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,xsd.jgbm,xsd.bbh,kjrq
	from djgl_xsdxx xsd
	where xfsh=v_xfsh and gfsh=v_gfsh and fplx=v_fplx and ywxt=v_ywxt and ywlx=v_ywlx and
	xsd.jspbh=v_jspbh and kpdh=v_kpdh and qdbz=v_qdbz and jgbm=v_jgbm and hjje<=v_dzxezp
	and xsdzt='0' and hjje>0 and zfbz='0' and dkbz='0';  
 
    -- 获取对应的负单
	call pro_xsd_zp_fdhq(v_xfsh,v_gfsh,v_fplx,v_ywxt,v_ywlx,v_jspbh,v_kpdh,v_qdbz,v_jgbm);
 
	call pro_xsdcl(v_dzxezp,v_qdbz,v_fplx,v_ywxt);
    
end loop;	
 
close xsdxx_zp_cursor;

insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsd_zdcl_zp','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单正单处理专票合并信息: 成功',sysdate(),concat('传入参数cs_xfsh：',cs_xfsh,'传入参数cs_fplx：',cs_fplx),
    concat('生成待开数量：',v_1));

commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsd_zdcl_pp
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsd_zdcl_pp`;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `pro_xsd_zdcl_pp`(cs_xfsh varchar(100),cs_fplx varchar(2))
BEGIN
declare xsdxx_zp_cursor_flag int default 0;
declare beginTime datetime default now();
declare error_code varchar(100);
declare error_msg text;
declare error_xx varchar(4000);
-- 用于信息筛选，存于游标xsdxx_zp_cursor
declare v_xfsh varchar(100);
declare v_gfmc varchar(200);
declare v_fplx varchar(2);
declare v_ywxt varchar(20);
declare v_ywlx varchar(50);
declare v_jspbh varchar(200);
declare v_kpdh varchar(200);
declare v_qdbz varchar(1);
declare v_jgbm varchar(100);

-- 获取开票服务器号
declare v_kpfwqh varchar(100);
declare v_sum_hjje decimal(16,2);

-- 用于存取插入表的ID
declare v_dk_xsdid varchar(100);

-- 存取所有可以抵消的负单的和
declare v_fd_hjje decimal(16,2);
 
-- 插入（详见销货清单）所需字段
declare v_qd_se decimal(16,2);
declare v_qd_je decimal(16,2);

-- 设置执行次数
declare v_1 varchar(100);

-- 用于统计税率
declare v_hjse decimal(16,2);

-- 存取限额
declare v_dzxezp decimal(16,2);

declare xsdxx_zp_cursor cursor for
select xsd.xfsh,xsd.gfmc,xsd.fplx,xsd.ywxt,xsd.ywlx,xsd.jspbh,xsd.kpdh,xsd.qdbz,xsd.jgbm
from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh)
where xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx and xsd.xsdzt='0' and xsd.hjje>0 and xsd.zfbz='0' and xsd.dkbz='0' and xsd.hjje<=jsp.dzxezp
group by xsd.xfsh,xsd.gfmc,xsd.fplx,xsd.ywxt,xsd.ywlx,xsd.jspbh,xsd.kpdh,xsd.qdbz,xsd.jgbm;

-- 遇到sqlexception,sqlwarning错误立即退出
declare exit handler for sqlexception,sqlwarning
begin
	rollback;
	GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
	set error_xx=concat('pro_xsd_zdcl_pp','错误代码：',error_code,'错误信息：',error_msg);
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
	values('pro_xsd_zdcl_pp','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理普票合并信息: ',error_xx),
		sysdate(),concat('传入参数cs_xfsh：',cs_xfsh), concat('传入参数cs_fplx：',cs_fplx));
	commit;
end;

-- 当游标循环遍历结束，结束游标
declare continue handler for not found set xsdxx_zp_cursor_flag =1;
set autocommit=0;

START TRANSACTION;

-- 搭建临时表
call pro_xsd_tmp_table();

set v_1 = 0;

open xsdxx_zp_cursor;
loop_xsdxx_zp:loop
	fetch xsdxx_zp_cursor
	into v_xfsh,v_gfmc,v_fplx,v_ywxt,v_ywlx,v_jspbh,v_kpdh,v_qdbz,v_jgbm;
 
	if xsdxx_zp_cursor_flag=1 then
 		leave loop_xsdxx_zp;
 	end if;
    
    set v_1 = v_1 + 1;
 
	-- 清空tmp_xsd_zdcl_zp表内容
	delete from tmp_xsd_zdcl_zp;
 
	-- 插入信息
	if exists(select * from jkgl_jspxx where nsrsbh=v_xfsh and jspbh=v_jspbh) then
		select kpfwqh into v_kpfwqh from jkgl_jspxx where nsrsbh=v_xfsh and jspbh=v_jspbh;
        select dzxezp into v_dzxezp from jkgl_jspxx where nsrsbh=v_xfsh and jspbh=v_jspbh;
	else    
		set v_kpfwqh=NULL;
	end if;
    
	insert into tmp_xsd_zdcl_zp(tmp_id,tmp_kpfwqh,tmp_jspbh,tmp_kpdh,tmp_fpzl,tmp_djbh,tmp_gfmc,tmp_gfsh,tmp_gfdzdh,tmp_gfyhzh,
	tmp_xfmc,tmp_xfsh,tmp_xfdzdh,tmp_xfyhzh,tmp_hjje,tmp_hjse,tmp_bz,tmp_fhr,tmp_skr,tmp_dybz,tmp_qdbz,tmp_lrrq,
	tmp_lrryid,tmp_lrrymc,tmp_lryhid,tmp_by1,tmp_by2,tmp_lsh,tmp_ywlx,tmp_ywxt,tmp_jgbm,tmp_bbh,tmp_kjrq)
	select xsd.id,v_kpfwqh,xsd.jspbh,kpdh,fplx,djbh,gfmc,gfsh,gfdzdh,gfyhzh,xfmc,xfsh,xfdzdh,xfyhzh,hjje,hjse,bz,
	fhr,skr,dybz,qdbz,now(),xsd.lrryid,xsd.lrrymc,xsd.lryhid,xsd.by1,xsd.by2,xsd.lsh,ywlx,ywxt,xsd.jgbm,xsd.bbh,kjrq
	from djgl_xsdxx xsd
	where xfsh=v_xfsh and gfmc=v_gfmc and fplx=v_fplx and ywxt=v_ywxt and ywlx=v_ywlx and
	xsd.jspbh=v_jspbh and kpdh=v_kpdh and qdbz=v_qdbz and jgbm=v_jgbm and hjje<=v_dzxezp
	and xsdzt='0' and hjje>0 and zfbz='0' and dkbz='0';  
 
	call pro_xsdcl(v_dzxezp,v_qdbz,v_fplx,v_ywxt);
    
end loop;	
 
close xsdxx_zp_cursor;

insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsd_zdcl_pp','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单正单处理普票合并信息: 成功',sysdate(),concat('传入参数cs_xfsh：',cs_xfsh,'传入参数cs_fplx：',cs_fplx),
    concat('生成待开数量：',v_1));

commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsd_zpcf
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsd_zpcf`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsd_zpcf`(cs_xfsh varchar(100),cs_fplx varchar(2))
BEGIN
	-- 专票-拆分
    declare zpcf_flag int default 0;
    declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
    
    -- 用于游标zpcf_cursor
    declare v_id varchar(100);
    declare v_dzxezp decimal(16,2);
    declare v_hjje decimal(16,2);
    
    declare v_xfsh varchar(100);
	declare v_gfsh varchar(200);
	declare v_fplx varchar(2);
	declare v_ywxt varchar(20);
	declare v_ywlx varchar(50);
	declare v_jspbh varchar(200);
	declare v_kpdh varchar(200);
	declare v_qdbz varchar(1);
	declare v_jgbm varchar(100);

	declare zpcf_cursor cursor for
    select  xsd.id,jsp.dzxezp,xsd.hjje,xsd.xfsh,xsd.gfsh,xsd.fplx,xsd.ywxt,xsd.ywlx,xsd.jspbh,xsd.kpdh,xsd.qdbz,xsd.jgbm 
    from djgl_xsdxx xsd left join jkgl_jspxx jsp 
    on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
    where jsp.dzxezp < xsd.hjje and xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx and xsd.xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0'
    union
    select  xsd.id,jsp.dzxezp,xsd.hjje,xsd.xfsh,xsd.gfsh,xsd.fplx,xsd.ywxt,xsd.ywlx,xsd.jspbh,xsd.kpdh,xsd.qdbz,xsd.jgbm 
    from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
    where qdbz='0' and (select count(*) from djgl_xsdxx_mx where xsdjid=xsd.id)>8 and xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx
	and xsd.xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0'
    union
    select  xsd.id,jsp.dzxezp,xsd.hjje,xsd.xfsh,xsd.gfsh,xsd.fplx,xsd.ywxt,xsd.ywlx,xsd.jspbh,xsd.kpdh,xsd.qdbz,xsd.jgbm 
    from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
    where qdbz='1' and (select count(*) from djgl_xsdxx_mx where xsdjid=xsd.id)>6000 and xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx
    and xsd.xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0';
    
    -- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_xsd_zpcf','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_xsd_zpcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理专票拆分信息: ',error_xx),
		sysdate(),concat('传入参数cs_xfsh：',cs_xfsh), concat('传入参数cs_fplx：',cs_fplx));
		commit;
	end;
    
    -- 当游标循环遍历结束，结束游标
	declare continue handler for not found set zpcf_flag =1;
	set autocommit=0;
    
    START TRANSACTION;
    
    -- 搭建临时表
	call pro_xsd_tmp_table();
    
    open zpcf_cursor;
    loop_zpcf:loop
		fetch zpcf_cursor into v_id,v_dzxezp,v_hjje,v_xfsh,v_gfsh,v_fplx,v_ywxt,v_ywlx,v_jspbh,v_kpdh,v_qdbz,v_jgbm;
        
        if zpcf_flag=1 then
			leave loop_zpcf;
		end if; 
        
		-- 获取对应的负单
		call pro_xsd_zp_fdhq(v_xfsh,v_gfsh,v_fplx,v_ywxt,v_ywlx,v_jspbh,v_kpdh,v_qdbz,v_jgbm);
	
		-- 相关明细拆分
        call pro_mx_cf(v_id,v_qdbz,v_dzxezp,v_fplx,v_ywxt);
        
        -- 修改销售单状态
        update djgl_xsdxx set xsdzt= 4 where id =v_id and xsdzt= 0;
        
	end loop;
    close zpcf_cursor;
    
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsd_zpcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单正单处理专票拆分信息: 成功',sysdate(),concat('传入参数cs_xfsh：',cs_xfsh), concat('传入参数cs_fplx：',cs_fplx));
    
    commit;
    
END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsd_ppcf
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsd_ppcf`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsd_ppcf`(cs_xfsh varchar(100),cs_fplx varchar(2))
BEGIN
	-- 普票-拆分
    declare zpcf_flag int default 0;
    declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
    
    -- 用于游标zpcf_cursor
    declare v_id varchar(100);
    declare v_dzxezp decimal(16,2);  
	declare v_fplx varchar(2);
	declare v_qdbz varchar(1);
	declare v_ywxt varchar(20);

	declare zpcf_cursor cursor for
    select  xsd.id,jsp.dzxezp,xsd.fplx,xsd.qdbz,xsd.ywxt
    from djgl_xsdxx xsd left join jkgl_jspxx jsp 
    on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
    where jsp.dzxezp < xsd.hjje and xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx and xsd.xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0'
    union
    select  xsd.id,jsp.dzxezp,xsd.fplx,xsd.qdbz,xsd.ywxt
    from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
    where qdbz='0' and (select count(*) from djgl_xsdxx_mx where xsdjid=xsd.id)>8 and xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx
	and xsd.xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0'
    union
    select  xsd.id,jsp.dzxezp,xsd.fplx,xsd.qdbz,xsd.ywxt
    from djgl_xsdxx xsd left join jkgl_jspxx jsp on (xsd.xfsh=jsp.nsrsbh and xsd.jspbh=jsp.jspbh) 
    where qdbz='1' and (select count(*) from djgl_xsdxx_mx where xsdjid=xsd.id)>6000 and xsd.xfsh=cs_xfsh and xsd.fplx=cs_fplx
    and xsd.xsdzt='0' and xsd.zfbz='0' and xsd.dkbz='0';
    
    -- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_xsd_zpcf','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_xsd_zpcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理普票拆分信息: ',error_xx),
		sysdate(),concat('传入参数cs_xfsh：',cs_xfsh), concat('传入参数cs_fplx：',cs_fplx));
		commit;
	end;
    
    -- 当游标循环遍历结束，结束游标
	declare continue handler for not found set zpcf_flag =1;
	set autocommit=0;
    
    START TRANSACTION;
    
    -- 搭建临时表
	call pro_xsd_tmp_table();
    
    open zpcf_cursor;
    loop_zpcf:loop
		fetch zpcf_cursor into v_id,v_dzxezp,v_fplx,v_qdbz,v_ywxt;
        
        if zpcf_flag=1 then
			leave loop_zpcf;
		end if; 
	
		-- 相关明细拆分
        call pro_mx_cf(v_id,v_qdbz,v_dzxezp,v_fplx,v_ywxt);
        
        -- 修改销售单状态
        update djgl_xsdxx set xsdzt= 4 where id =v_id and xsdzt= 0;
        
	end loop;
    close zpcf_cursor;
    
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsd_zpcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'销售单正单处理普票拆分信息: 成功',sysdate(),concat('传入参数cs_xfsh：',cs_xfsh), concat('传入参数cs_fplx：',cs_fplx));
    
    commit;
    
END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsdcl_job_ppcf
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsdcl_job_ppcf`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsdcl_job_ppcf`()
BEGIN
-- 销售单-正单-普票拆分
declare v_nsrsbh varchar(100);
declare xxfp_cursor_flag int default 0;
-- 用于异常排查
declare beginTime datetime default now();
declare error_code varchar(100);
declare error_msg text;
declare error_xx varchar(4000);

declare fp_cursor cursor for
select nsrsbh from xtgl_xfxx;

-- 遇到sqlexception,sqlwarning错误立即退出
declare exit handler for sqlexception,sqlwarning
begin
	rollback;
	GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
	set error_xx=concat('pro_xsdcl_job_ppcf','错误代码：',error_code,'错误信息：',error_msg);
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
	values('pro_xsdcl_job_ppcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理-普票拆分: ',error_xx),
	sysdate(),null, null);
	commit;
end;

-- 当游标到最后一条时，终止循环
declare continue handler for not found set xxfp_cursor_flag =1;
set autocommit=0;

START TRANSACTION;

open fp_cursor;
loop_fp:loop
	fetch fp_cursor
    into v_nsrsbh;
    
		if xxfp_cursor_flag=1 then
			leave loop_fp;
		end if;
    
		-- 销售单正单普票拆分
		call pro_xsd_ppcf(v_nsrsbh,'2');
    
end loop;

close fp_cursor;

insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsdcl_job_ppcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
'销售单正单处理-普票拆分: 成功',sysdate(),null,null);

commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsdcl_job_pphb
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsdcl_job_pphb`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsdcl_job_pphb`()
BEGIN
-- 销售单-正单-普票合并
declare v_nsrsbh varchar(100);
declare xxfp_cursor_flag int default 0;
-- 用于异常排查
declare beginTime datetime default now();
declare error_code varchar(100);
declare error_msg text;
declare error_xx varchar(4000);

declare fp_cursor cursor for
select nsrsbh from xtgl_xfxx;

-- 遇到sqlexception,sqlwarning错误立即退出
declare exit handler for sqlexception,sqlwarning
begin
	rollback;
    GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
	set error_xx=concat('pro_xsdcl_job_pphb','错误代码：',error_code,'错误信息：',error_msg);
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
	values('pro_xsdcl_job_pphb','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理-普票合并: ',error_xx),
	sysdate(),null, null);
	commit;
end;

-- 当游标到最后一条时，终止循环
declare continue handler for not found set xxfp_cursor_flag =1;
set autocommit=0;

START TRANSACTION;

open fp_cursor;
loop_fp:loop
	fetch fp_cursor
    into v_nsrsbh;
    
		if xxfp_cursor_flag=1 then
			leave loop_fp;
		end if;
    
		-- 销售单正单普票合并
		call pro_xsd_zdcl_pp(v_nsrsbh,'2');
    
end loop;

close fp_cursor;

insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsdcl_job_pphb','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
'销售单正单处理-普票合并: 成功',sysdate(),null,null);


commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsdcl_job_zpcf
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsdcl_job_zpcf`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsdcl_job_zpcf`()
BEGIN
-- 销售单-正单-专票拆分
declare v_nsrsbh varchar(100);
declare xxfp_cursor_flag int default 0;
-- 用于异常排查
declare beginTime datetime default now();
declare error_code varchar(100);
declare error_msg text;
declare error_xx varchar(4000);

declare fp_cursor cursor for
select nsrsbh from xtgl_xfxx;

-- 遇到sqlexception,sqlwarning错误立即退出
declare exit handler for sqlexception,sqlwarning
begin
	rollback;
    GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
	set error_xx=concat('pro_xsdcl_job_zpcf','错误代码：',error_code,'错误信息：',error_msg);
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
	values('pro_xsdcl_job_zpcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理-专票拆分: ',error_xx),
	sysdate(),null, null);
	commit;
end;

-- 当游标到最后一条时，终止循环
declare continue handler for not found set xxfp_cursor_flag =1;
set autocommit=0;

START TRANSACTION;

open fp_cursor;
loop_fp:loop
	fetch fp_cursor
    into v_nsrsbh;
    
		if xxfp_cursor_flag=1 then
			leave loop_fp;
		end if;
    
		-- 销售单正单专票拆分
		call pro_xsd_zpcf(v_nsrsbh,'0');
    
end loop;

close fp_cursor;

insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsdcl_job_zpcf','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
'销售单正单处理-专票拆分: 成功',sysdate(),null,null);

commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_xsdcl_job_zphb
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_xsdcl_job_zphb`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_xsdcl_job_zphb`()
BEGIN
-- 销售单-正单-专票合并
declare v_nsrsbh varchar(100);
declare xxfp_cursor_flag int default 0;
-- 用于异常排查
declare beginTime datetime default now();
declare error_code varchar(100);
declare error_msg text;
declare error_xx varchar(4000);

declare fp_cursor cursor for
select nsrsbh from xtgl_xfxx;

-- 遇到sqlexception,sqlwarning错误立即退出
declare exit handler for sqlexception,sqlwarning
begin
	rollback;
    GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
	set error_xx=concat('pro_xsdcl_job_zphb','错误代码：',error_code,'错误信息：',error_msg);
	insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
	values('pro_xsdcl_job_zphb','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('销售单正单处理-专票合并: ',error_xx),
	sysdate(),null, null);
	commit;
end;

-- 当游标到最后一条时，终止循环
declare continue handler for not found set xxfp_cursor_flag =1;
set autocommit=0;

START TRANSACTION;

open fp_cursor;
loop_fp:loop
	fetch fp_cursor
    into v_nsrsbh;
    
		if xxfp_cursor_flag=1 then
			leave loop_fp;
		end if;
    
		-- 销售单正单专票合并
		call pro_xsd_zdcl_zp(v_nsrsbh,'0');
    
end loop;

close fp_cursor;

insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_xsdcl_job_zphb','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
'销售单正单处理-专票合并: 成功',sysdate(),null,null);

commit;

END;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for pro_fdcl
-- ----------------------------
DROP PROCEDURE IF EXISTS `pro_fdcl`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `pro_fdcl`()
BEGIN
	declare fd_cursor_flag int default 0;
    
	declare beginTime datetime default now();
	declare error_code varchar(100);
	declare error_msg text;
	declare error_xx varchar(4000);
    
    declare v_id varchar(100);
    
	declare v_sl bigint(100);
    
    declare fd_cursor cursor for
    select id from djgl_xsdxx where hjje<0 and xsdzt='0';
    
    -- 遇到sqlexception,sqlwarning错误立即退出
	declare exit handler for sqlexception,sqlwarning
	begin
		rollback;
        GET DIAGNOSTICS CONDITION 1 error_code = RETURNED_SQLSTATE, error_msg = MESSAGE_TEXT;
		set error_xx=concat('pro_fdcl','错误代码：',error_code,'错误信息：',error_msg);
		insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq, by1, by2)
		values('pro_fdcl','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'1',concat('向负单表存入信息: ',error_xx),
		sysdate(),concat('无参数'), concat('本次共传入负单数量：',v_sl));
		commit;
	end;
    
    -- 当游标到最后一条时，终止循环
	declare continue handler for not found set fd_cursor_flag =1;
	set autocommit=0;
    
    set v_sl = 0;
    
    open fd_cursor;
    loop_fd:loop
		fetch fd_cursor into v_id;
        
        if fd_cursor_flag=1 then
			leave loop_fd;
		end if;
        
        if(select count(*) from djgl_fdxx where xsdid=v_id)=0 then
        
			insert into djgl_fdxx(xsdid,lphm,lpdm,lrrq)
			select xsd.id,pjgx.fphm,pjgx.fpdm,now() from djgl_xsdxx xsd left join kpgl_xxfp_pjgx pjgx on (xsd.ydh=pjgx.xsdjbh and xsd.ywxt=pjgx.ywxt)
            where xsd.id=v_id;
        
        end if;
        
        if(select count(*) from djgl_fdxx where xsdid=v_id and czbz='0')=1 then
        
			delete from djgl_fdxx where xsdid=v_id;
        
			insert into djgl_fdxx(xsdid,lphm,lpdm,lrrq)
			select xsd.id,pjgx.fphm,pjgx.fpdm,now() from djgl_xsdxx xsd left join kpgl_xxfp_pjgx pjgx on (xsd.ydh=pjgx.xsdjbh and xsd.ywxt=pjgx.ywxt)
            where xsd.id=v_id;
        
        end if;
        
        set v_sl = v_sl + 1 ;
        
	end loop;
    close fd_cursor;
    
    insert into xtgl_htrz(dxmc, cxlx, zxrq, dyrq, jsrq, hs, zxzt, ms, lrrq,by1,by2) values('pro_fdcl','存储过程',beginTime,sysdate(),sysdate(),(sysdate()-beginTime)*24*60*60*1000,'0',
	'向负单表存入信息: 成功',sysdate(),concat('无参数'), concat('本次共传入负单数量：',v_sl));
    
    commit;
    
END;;
DELIMITER ;
