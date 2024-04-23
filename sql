CREATE TYPE listchar AS TABLE OF VARCHAR(1000);

CREATE OR REPLACE FUNCTION analizestringname(name_r3 VARCHAR) RETURNS listchar AS
$$
DECLARE
    listt listchar := '{}';
    fchar CHAR(1);
    comname VARCHAR(20) := NULL;
    corpos INT;
    countchar INT := 0;
    k INT := 1;
BEGIN
    countchar := LENGTH(name_r3);
    corpos := INSTR(lower(name_r3), 'скважин');
    
    IF (corpos != 0) THEN
        corpos := corpos + 7;
        
        IF (corpos <= countchar) THEN
            FOR i IN corpos..countchar LOOP
                fchar := SUBSTRING(name_r3 FROM i FOR 1);
                
                IF (fchar SIMILAR TO '[0-9]') THEN
                    comname := comname  fchar;
                END IF;
                
                IF ((ASCII(fchar) >= 65 AND ASCII(fchar) <= 90) OR
                    (ASCII(fchar) >= 97 AND ASCII(fchar) <= 122) OR
                    (ASCII(fchar) >= 192 AND ASCII(fchar) <= 255)) AND LENGTH(comname) > 0 THEN
                    comname := comname  fchar;
                END IF;
                
                IF ((fchar = ', ' OR fchar = '. ' OR fchar = ' ' OR i = countchar) AND LENGTH(comname) > 0) THEN
                    listt[k] := comname;
                    comname := NULL;
                    k := k + 1;
                END IF;
            END LOOP;
        END IF;
    ELSE
        listt[1] := REPLACE(name_r3, '''', '');
    END IF;
    
    RETURN listt;
END;
$$
LANGUAGE plpgsql;

---------------------

CREATE TYPE listchar AS TABLE OF VARCHAR(1000);

CREATE OR REPLACE FUNCTION getLinkPKS(listTable VARCHAR, year_pks VARCHAR, group_pks VARCHAR, employer_pks VARCHAR, blink VARCHAR) 
RETURNS SETOF recPKS AS
$$
DECLARE
    rec recPKS;    
    employer_t VARCHAR(60) := null;  
    mslink_t INT := 0;  
    name_t VARCHAR(4000) := null;  
    type_t VARCHAR(25) := null;  
    powers_t NUMERIC(10,3) := 0;  
    fieldoil_t VARCHAR(100) := null;  
    yearpks_t VARCHAR(100) := null;    
    sql_select VARCHAR(2000);  
    sql_add VARCHAR(500) := null;  
    table_name VARCHAR(100);  
    lTable listchar;  
    listName listchar;  
    first_char CHAR(1);  
    it INT := 1;
    kt INT := 1;    
    rowfound INT := 0;  
    TYPE curtype IS REF CURSOR;
    cur1 curtype;  
    r3proc INT;  
    cLink INT := 0;  
    countlink INT := 0;  
    tablecode INT := 0;  
    tablenamefromlink VARCHAR(60);
BEGIN
    WHILE (LENGTH(listTable) > it) LOOP
        table_name := RTRIM(SUBSTRING(listTable FROM it FOR INSTR(listTable, ',', it) - it), ',');    
        lTable(kt) := table_name;
        it := INSTR(listTable, ',', it) + 1;
        kt := kt + 1;
    END LOOP;

    r3proc := 100;

    FOR recr3 IN (
        SELECT DISTINCT r3p.enterp_name, r3p.field_name, r3ps.name, r3ps.pks_year, r3ps.po_pks, r3ps.id_sub,
                        r3ps.measureunit, r3ps.pks_power, r3ps.pks_group_name, r3ps.pks_level, pg3.layer_id, pg3.keyid,
                        pksstat.sub_id AS status_subid, pksstat.status_id
        FROM gis_links.r3_pks r3p
        LEFT JOIN gis_links.r3_pks_sub r3ps ON r3p.id_contractor = r3ps.id_contractor AND r3p.id_cypher = r3ps.id_cypher
        LEFT JOIN gis_links.r3_pks_gis pg3 ON r3ps.id_sub = pg3.r3_mesto
        LEFT JOIN gis_links.r3_pks_statuslink pksstat ON pksstat.sub_id = r3ps.id_sub
        WHERE r3p.enterp_name = employer_pks AND r3ps.pks_year = year_pks AND r3ps.pks_group_name = group_pks
    ) LOOP
        IF (blink = '0' OR recr3.layer_id IS NULL) THEN
            IF (blink = '0' AND recr3.layer_id IS NOT NULL) THEN
                SELECT t.views INTO tablenamefromlink FROM geo.tables t WHERE t.mslink = recr3.layer_id;
                IF (tablenamefromlink IS NOT NULL) THEN
                    IF (LOWER(tablenamefromlink) = 'geo.пкс_лэп') THEN
                        sql_select := 'SELECT название,
                                            мощность,
                                            mslink,
                                            NULL AS тип,
                                            месторождение
                                        FROM '  tablenamefromlink  '
                                        WHERE mslink = '  recr3.keyid;
                    ELSE
                        sql_select := 'SELECT название,
                                            мощность,
                                            mslink,
                                            тип,
                                            месторождение
                                        FROM '  tablenamefromlink  '
                                        WHERE mslink = '  recr3.keyid;
                    END IF;
                    EXECUTE sql_select INTO name_t, powers_t, mslink_t, type_t, fieldoil_t;
                END IF;
                rec.po_pks := recr3.po_pks;
                rec.id_sub := recr3.id_sub;
                rec.name_r3 := recr3.name;
                rec.unit_r3 := recr3.measureunit;
                rec.powers_r3 := recr3.pks_power;
                rec.oilfield_r3 := recr3.field_name;
                rec.powers_gis := powers_t;
                IF (type_t = 'куст скважин' OR type_t = 'скважина одиночная' OR type_t = 'скважина разведочная') THEN   
                    rec.bush_r3 := name_t;
                ELSE
                    rec.bush_r3 := null;
                END IF;
                rec.name_gis := name_t;
                rec.oilfield_gis := fieldoil_t;
                rec.mslink := mslink_t;
                rec.nametable_gis := tablenamefromlink;

rec.checklink := 1; -- Привязана
                RETURN NEXT rec;
                name_t := null;
                powers_t := 0;
                mslink_t := null;
                fieldoil_t := null;
                type_t := null;
            ELSIF (recr3.status_subid IS NULL OR recr3.status_id = 3) THEN
                IF (recr3.pks_group_name = 'Объ-ты жилья') THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                first_char := SUBSTRING(LTRIM(recr3.name), 1, 1);
                IF (f_number(first_char) = 1) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;    
                IF (ASCII(first_char) <= 192 AND ASCII(first_char) >= 255) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                IF (recr3.pks_group_name = 'Объ-ты произ-го обслуж-я' OR recr3.pks_group_name = 'Нефтепромысловые объекты') THEN
                    IF (recr3.pks_level != 1) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    IF (INSTR(LOWER(recr3.name), 'реконструк') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    ELSIF (INSTR(LOWER(recr3.name), 'техническ') != 0 AND INSTR(LOWER(recr3.name), 'перевооруж') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                END IF;
                IF (recr3.pks_group_name = 'Энергоснабжение' OR recr3.pks_group_name = 'Труб-ы, обуст-во кустов' OR recr3.pks_group_name = 'Авто-ые дороги и мосты') THEN
                    IF (recr3.pks_level > 2) THEN  
                        r3proc := 0;
                        cLink := 2;
                    END IF;      
                    IF (recr3.pks_group_name = 'Авто-ые дороги и мосты') THEN
                        IF (INSTR(LOWER(recr3.name), 'внутрипромысл') != 0) THEN
                            r3proc := 0;
                            cLink := 2;
                        END IF;
                    END IF;
                    IF (recr3.pks_group_name = 'Труб-ы, обуст-во кустов') THEN
                        IF (INSTR(LOWER(recr3.name), 'площадка') != 0 AND INSTR(LOWER(recr3.name), 'кустовая') != 0) THEN
                            r3proc := 0;
                            cLink := 2;
                        END IF;
                    END IF;            
                    IF (INSTR(LOWER(recr3.name), 'обустройство') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    IF (INSTR(LOWER(recr3.name), 'благоустройство') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    IF (INSTR(LOWER(recr3.name), 'перевод') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                END IF;
                IF (INSTR(LOWER(recr3.name), 'шурф') != 0 AND INSTR(LOWER(recr3.name), 'куст') != 0) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                IF (INSTR(LOWER(recr3.name), 'обустройство') != 0 AND INSTR(LOWER(recr3.name), 'шурф') != 0) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                IF (recr3.status_id = 3) THEN
                    cLink := 3;
                END IF;
                IF (r3proc = 100) THEN
                    listName := analizestringname(recr3.name);
                    IF (listName.count > 0) THEN
                        FOR j IN listName.FIRST..listName.LAST LOOP                  
                            FOR i IN lTable.FIRST..lTable.LAST LOOP
                                IF (LOWER(lTable(i)) = 'geo.пкс_лэп') THEN
                                    sql_select := 'SELECT название,
                                                        мощность,

mslink,
                                                        NULL AS тип,
                                                        месторождение
                                                    FROM '  lTable(i)  '
                                                    WHERE название = '''  listName(j) 
                                                        ''' AND заказчик = '''  recr3.enterp_name 
                                                        ''' AND месторождение = '''  recr3.field_name 
                                                        ''' AND год_пкс = '''  year_pks 
                                                        ''' AND группа_пкс = '''  recr3.pks_group_name  '''';
                                ELSE
                                    sql_select := 'SELECT название,
                                                        мощность,
                                                        mslink,
                                                        тип,
                                                        месторождение
                                                    FROM '  lTable(i)  '
                                                    WHERE название = '''  listName(j) 
                                                        ''' AND заказчик = '''  recr3.enterp_name 
                                                        ''' AND месторождение = '''  recr3.field_name 
                                                        ''' AND год_пкс = '''  year_pks 
                                                        ''' AND группа_пкс = '''  recr3.pks_group_name  '''';
                                END IF;
                                OPEN cur1 FOR sql_select;
                                LOOP
                                    FETCH cur1 INTO name_t, powers_t, mslink_t, type_t, fieldoil_t;
                                    IF (name_t IS NOT NULL) THEN
                                        rec.po_pks := recr3.po_pks;
                                        rec.id_sub := recr3.id_sub;
                                        rec.name_r3 := recr3.name;
                                        rec.unit_r3 := recr3.measureunit;
                                        rec.powers_r3 := recr3.pks_power;
                                        rec.oilfield_r3 := recr3.field_name;                          
                                        rec.powers_gis := powers_t;
                                        IF (type_t = 'куст скважин' OR type_t = 'скважина одиночная' OR type_t = 'скважина разведочная') THEN
                                            rec.bush_r3 := listName(j);
                                        ELSE
                                            rec.bush_r3 := null;
                                        END IF;
                                        rec.name_gis := name_t;
                                        rec.oilfield_gis := fieldoil_t;
                                        rec.mslink := mslink_t;
                                        rec.nametable_gis := lTable(i);
                                        rec.checklink := cLink;
                                        RETURN NEXT rec;
                                        name_t := null;
                                        powers_t := 0;
                                        mslink_t := null;
                                        fieldoil_t := null;
                                        type_t := null;
                                    END IF;  
                                    EXIT WHEN NOT FOUND;    
                                END LOOP;
                                CLOSE cur1;
                            END LOOP;
                        END LOOP;
                    END IF;
                    IF (rowfound = 0) THEN
                        rec.po_pks := recr3.po_pks;
                        rec.id_sub := recr3.id_sub;
                        rec.name_r3 := recr3.name;
                        rec.unit_r3 := recr3.measureunit;

rec.powers_r3 := recr3.pks_power;
                        rec.oilfield_r3 := recr3.field_name;
                        rec.powers_gis := null;              
                        rec.bush_r3 := null;
                        IF (INSTR(LOWER(recr3.name), 'куст') = 1) THEN
                            rec.bush_r3 := listName(1);
                            FOR j IN 2..listName.LAST LOOP  
                                rec.bush_r3 := rec.bush_r3  ','  listName(j);    
                            END LOOP;
                        END IF;
                        rec.name_gis := null;
                        rec.oilfield_gis := null;
                        rec.mslink := null;
                        rec.nametable_gis := null;
                        rec.checklink := cLink;
                        RETURN NEXT rec;
                    END IF;
                    rowfound := 0;
                ELSE  
                    rec.po_pks := recr3.po_pks;
                    rec.id_sub := recr3.id_sub;
                    rec.name_r3 := recr3.name;
                    rec.unit_r3 := recr3.measureunit;
                    rec.powers_r3 := recr3.pks_power;
                    rec.oilfield_r3 := recr3.field_name;
                    rec.powers_gis := null;
                    rec.bush_r3 := null;
                    rec.name_gis := null;
                    rec.oilfield_gis := null;
                    rec.mslink := null;
                    rec.nametable_gis := null;
                    rec.checklink := cLink;
                    RETURN NEXT rec;
                END IF;
                r3proc := 100;
                cLink := 0;
            ELSE          
                rec.po_pks := recr3.po_pks;
                rec.id_sub := recr3.id_sub;
                rec.name_r3 := recr3.name;
                rec.unit_r3 := recr3.measureunit;
                rec.powers_r3 := recr3.pks_power;
                rec.oilfield_r3 := recr3.field_name;
                rec.powers_gis := null;          
                rec.bush_r3 := null;
                IF (INSTR(LOWER(recr3.name), 'куст') = 1) THEN
                    listName := analizestringname(recr3.name);          
                    rec.bush_r3 := listName(1);            
                    FOR j IN 2..listName.LAST LOOP  
                        rec.bush_r3 := rec.bush_r3  ','  listName(j);    
                    END LOOP;
                END IF;        
                rec.name_gis := null;
                rec.oilfield_gis := null;
                rec.mslink := null;
                rec.nametable_gis := null;
                rec.checklink := recr3.status_id;
                RETURN NEXT rec;      
            END IF;
        END IF;
    END LOOP;   
END;
$$
LANGUAGE plpgsql;

---------------------


CREATE TYPE tablePKS AS (
    po_pks VARCHAR(100),
    id_sub INTEGER,
    name_r3 VARCHAR(4000),
    unit_r3 VARCHAR(60),
    powers_r3 NUMERIC(10,3),
    oilfield_r3 VARCHAR(100),
    powers_gis NUMERIC(10,3),
    bush_r3 VARCHAR(4000),
    name_gis VARCHAR(4000),
    oilfield_gis VARCHAR(100),
    mslink INTEGER,
    nametable_gis VARCHAR(100),
    checklink INTEGER
);

CREATE OR REPLACE FUNCTION getLinkPKS(listTable VARCHAR, year_pks VARCHAR, group_pks VARCHAR, employer_pks VARCHAR, blink VARCHAR) 
RETURNS SETOF tablePKS AS
$$
DECLARE
    rec tablePKS;    
    employer_t VARCHAR(60):=NULL;  
    mslink_t INTEGER := 0;  
    name_t VARCHAR(4000):=NULL;  
    type_t VARCHAR(25):=NULL;  
    powers_t NUMERIC(10,3) := 0;  
    fieldoil_t VARCHAR(100):=NULL;  
    yearpks_t VARCHAR(100):=NULL;    
    sql_select VARCHAR(2000);  
    sql_add VARCHAR(500):=NULL;  
    table_name VARCHAR(100);  
    lTable listchar;  
    listName listchar;  
    first_char VARCHAR(1);  
    it INTEGER := 1;
    kt INTEGER := 1;    
    rowfound INTEGER := 0;  
    r3proc INTEGER;  
    cLink INTEGER:=0;  
    countlink INTEGER:=0;  
    tablecode INTEGER:=0;  
    tablenamefromlink VARCHAR(60);
BEGIN
    WHILE (LENGTH(listTable) > it) LOOP
        table_name := RTRIM(SUBSTRING(listTable FROM it FOR INSTR(listTable, ',', it) - it), ',');    
        lTable(kt) := table_name;
        it := INSTR(listTable, ',', it) + 1;
        kt := kt + 1;
    END LOOP;
    
    r3proc := 100;
    
    FOR recr3 IN (SELECT DISTINCT r3p.enterp_name, r3p.field_name,r3ps.name, r3ps.pks_year, r3ps.po_pks, r3ps.id_sub,
                          r3ps.measureunit, r3ps.pks_power, r3ps.pks_group_name, r3ps.pks_level, pg3.layer_id, pg3.keyid,
                          pksstat.sub_id status_subid, pksstat.status_id
                         FROM gis_links.r3_pks r3p LEFT JOIN gis_links.r3_pks_sub r3ps
                         ON r3p.id_contractor = r3ps.id_contractor AND r3p.id_cypher = r3ps.id_cypher
                         LEFT JOIN gis_links.r3_pks_gis pg3
                         ON r3ps.id_sub = pg3.r3_mesto
                         LEFT JOIN gis_links.r3_pks_statuslink pksstat
                         ON pksstat.sub_id = r3ps.id_sub
                         WHERE r3p.enterp_name = employer_pks AND r3ps.pks_year = year_pks AND r3ps.pks_group_name = group_pks
                         --ORDER BY r3ps.id_cypher, r3ps.id_sub
                         )
    LOOP
        IF (blink = '0' OR recr3.layer_id IS NULL) THEN
            IF (blink = '0' AND recr3.layer_id IS NOT NULL) THEN
                SELECT t.views INTO tablenamefromlink FROM geo.tables t WHERE t.mslink = recr3.layer_id;
                IF (tablenamefromlink IS NOT NULL) THEN
                    IF (LOWER(tablenamefromlink) = 'geo.пкс_лэп') THEN
                        sql_select := 'SELECT название,
                                          мощность,
                                          mslink,
                                          NULL тип,
                                          месторождение
                                          FROM '  tablenamefromlink  '
                                          WHERE mslink = '  recr3.keyid;
                    ELSE
                        sql_select := 'SELECT название,
                                          мощность,
                                          mslink,
                                          тип,
                                          месторождение
                                          FROM '  tablenamefromlink  '
                                          WHERE mslink = '  recr3.keyid;
                    END IF;
                    EXECUTE sql_select INTO name_t, powers_t, mslink_t, type_t, fieldoil_t;
                END IF;
                rec.po_pks := recr3.po_pks;
                rec.id_sub := recr3.id_sub;
                rec.name_r3 := recr3.name;
                rec.unit_r3 := recr3.measureunit;
                rec.powers_r3 := recr3.pks_power;
                rec.oilfield_r3 := recr3.field_name;

rec.powers_gis := powers_t;
                IF (type_t = 'куст скважин' OR type_t = 'скважина одиночная' OR type_t = 'скважина разведочная') THEN   
                    rec.bush_r3 := name_t;
                ELSE
                    rec.bush_r3 := NULL;
                END IF;
                rec.name_gis := name_t;
                rec.oilfield_gis := fieldoil_t;
                rec.mslink := mslink_t;
                rec.nametable_gis := tablenamefromlink;
                rec.checklink := 1; -- Привязана
                RETURN NEXT rec;
                name_t := NULL;
                powers_t := 0;
                mslink_t := NULL;
                fieldoil_t := NULL;
                type_t := NULL;
            ELSIF (recr3.status_subid IS NULL OR recr3.status_id = 3) THEN
                IF (recr3.pks_group_name = 'Объ-ты жилья') THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                first_char := SUBSTRING(LTRIM(recr3.name), 1, 1);
                IF (f_number(first_char) = 1) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;    
                IF (ASCII(first_char) <= 192 AND ASCII(first_char) >= 255) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                IF (recr3.pks_group_name = 'Объ-ты произ-го обслуж-я' OR recr3.pks_group_name = 'Нефтепромысловые объекты') THEN
                    IF (recr3.pks_level != 1) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    IF (INSTR(LOWER(recr3.name), 'реконструк') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    ELSIF (INSTR(LOWER(recr3.name), 'техническ') != 0 AND INSTR(LOWER(recr3.name), 'перевооруж') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                END IF;
                IF (recr3.pks_group_name = 'Энергоснабжение' OR recr3.pks_group_name = 'Труб-ы, обуст-во кустов' OR recr3.pks_group_name = 'Авто-ые дороги и мосты') THEN
                    IF (recr3.pks_level > 2) THEN  
                        r3proc := 0;
                        cLink := 2;
                    END IF;      
                    IF (recr3.pks_group_name = 'Авто-ые дороги и мосты') THEN
                        IF (INSTR(LOWER(recr3.name), 'внутрипромысл') != 0) THEN
                            r3proc := 0;
                            cLink := 2;
                        END IF;
                    END IF;
                    IF (recr3.pks_group_name = 'Труб-ы, обуст-во кустов') THEN
                        IF (INSTR(LOWER(recr3.name), 'площадка') != 0 AND INSTR(LOWER(recr3.name), 'кустовая') != 0) THEN
                            r3proc := 0;
                            cLink := 2;
                        END IF;
                    END IF;            
                    IF (INSTR(LOWER(recr3.name), 'обустройство') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    IF (INSTR(LOWER(recr3.name), 'благоустройство') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    IF (INSTR(LOWER(recr3.name), 'перевод') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                END IF;
                IF (INSTR(LOWER(recr3.name), 'шурф') != 0 AND INSTR(LOWER(recr3.name), 'куст') != 0) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                IF (INSTR(LOWER(recr3.name), 'обустройство') != 0 AND INSTR(LOWER(recr3.name), 'шурф') != 0) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                IF (recr3.status_id = 3) THEN
                    cLink := 3;
                END IF;
                IF (r3proc = 100) THEN

listName := analizestringname(recr3.name);
                    IF listName.count > 0 THEN
                        FOR j IN listName.FIRST..listName.LAST LOOP                  
                            FOR i IN lTable.FIRST..lTable.LAST LOOP
                                IF (LOWER(lTable(i)) = 'geo.пкс_лэп') THEN
                                    sql_select := 'SELECT название,
                                                      мощность,
                                                      mslink,
                                                      NULL тип,
                                                      месторождение
                                                      FROM '  lTable(i)  '
                                                      WHERE название = '''  listName(j) 
                                                            ''' AND заказчик = '''  recr3.enterp_name 
                                                            ''' AND месторождение = '''  recr3.field_name 
                                                            ''' AND год_пкс = '''  year_pks 
                                                            ''' AND группа_пкс = '''  recr3.pks_group_name  '''';
                                ELSE
                                    sql_select := 'SELECT название,
                                                      мощность,
                                                      mslink,
                                                      тип,
                                                      месторождение
                                                      FROM '  lTable(i)  '
                                                      WHERE название = '''  listName(j) 
                                                            ''' AND заказчик = '''  recr3.enterp_name 
                                                            ''' AND месторождение = '''  recr3.field_name 
                                                            ''' AND год_пкс = '''  year_pks 
                                                            ''' AND группа_пкс = '''  recr3.pks_group_name  '''';
                                END IF;
                                OPEN cur1 FOR sql_select;
                                LOOP
                                    FETCH cur1 INTO name_t, powers_t, mslink_t, type_t, fieldoil_t;
                                    IF (name_t IS NOT NULL) THEN
                                        rec.po_pks := recr3.po_pks;
                                        rec.id_sub := recr3.id_sub;
                                        rec.name_r3 := recr3.name;
                                        rec.unit_r3 := recr3.measureunit;
                                        rec.powers_r3 := recr3.pks_power;
                                        rec.oilfield_r3 := recr3.field_name;                          
                                        rec.powers_gis := powers_t;
                                        IF (type_t = 'куст скважин' OR type_t = 'скважина одиночная' OR type_t = 'скважина разведочная') THEN
                                            rec.bush_r3 := listName(j);
                                        ELSE
                                            rec.bush_r3 := NULL;
                                        END IF;
                                        rec.name_gis := name_t;
                                        rec.oilfield_gis := fieldoil_t;
                                        rec.mslink := mslink_t;
                                        rec.nametable_gis := lTable(i);
                                        rec.checklink := cLink;
                                        RETURN NEXT rec;
                                        name_t := NULL;
                                        powers_t := 0;
                                        mslink_t := NULL;
                                        fieldoil_t := NULL;
                                        type_t := NULL;
                                    END IF;

EXIT WHEN NOT FOUND;    
                                END LOOP;
                                CLOSE cur1;
                            END LOOP;
                        END LOOP;
                    END IF;
                    IF (rowfound = 0) THEN
                        rec.po_pks := recr3.po_pks;
                        rec.id_sub := recr3.id_sub;
                        rec.name_r3 := recr3.name;
                        rec.unit_r3 := recr3.measureunit;
                        rec.powers_r3 := recr3.pks_power;
                        rec.oilfield_r3 := recr3.field_name;
                        rec.powers_gis := NULL;              
                        rec.bush_r3 := NULL;
                        IF (INSTR(LOWER(recr3.name), 'куст') = 1) THEN
                            rec.bush_r3 := listName(1);
                            FOR j IN 2..listName.LAST LOOP  
                                rec.bush_r3 := rec.bush_r3  ','  listName(j);    
                            END LOOP;
                        END IF;
                        rec.name_gis := NULL;
                        rec.oilfield_gis := NULL;
                        rec.mslink := NULL;
                        rec.nametable_gis := NULL;
                        rec.checklink := cLink;
                        RETURN NEXT rec;
                    END IF;
                    rowfound := 0;
                ELSE  
                    rec.po_pks := recr3.po_pks;
                    rec.id_sub := recr3.id_sub;
                    rec.name_r3 := recr3.name;
                    rec.unit_r3 := recr3.measureunit;
                    rec.powers_r3 := recr3.pks_power;
                    rec.oilfield_r3 := recr3.field_name;
                    rec.powers_gis := NULL;
                    rec.bush_r3 := NULL;
                    rec.name_gis := NULL;
                    rec.oilfield_gis := NULL;
                    rec.mslink := NULL;
                    rec.nametable_gis := NULL;
                    rec.checklink := cLink;
                    RETURN NEXT rec;
                END IF;
                r3proc := 100;
                cLink := 0;
            ELSE          
                rec.po_pks := recr3.po_pks;
                rec.id_sub := recr3.id_sub;
                rec.name_r3 := recr3.name;
                rec.unit_r3 := recr3.measureunit;
                rec.powers_r3 := recr3.pks_power;
                rec.oilfield_r3 := recr3.field_name;
                rec.powers_gis := NULL;          
                rec.bush_r3 := NULL;
                IF (INSTR(LOWER(recr3.name), 'куст') = 1) THEN
                    listName := analizestringname(recr3.name);          
                    rec.bush_r3 := listName(1);            
                    FOR j IN 2..listName.LAST LOOP  
                        rec.bush_r3 := rec.bush_r3  ','  listName(j);    
                    END LOOP;
                END IF;        
                rec.name_gis := NULL;
                rec.oilfield_gis := NULL;
                rec.mslink := NULL;
                rec.nametable_gis := NULL;
                rec.checklink := recr3.status_id;
                RETURN NEXT rec;      
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-------------------------


CREATE OR REPLACE FUNCTION analizestringname(name_r3 varchar) RETURNS varchar[] AS $$
DECLARE
    listt varchar[];
    fchar varchar(1);
    comname varchar(20) := null;
    corpos integer;
    countchar integer := 0;
    k integer := 1;
BEGIN
    countchar := length(name_r3);
    corpos := strpos(lower(name_r3), 'скважин');
    
    IF (corpos != 0) THEN
        corpos := corpos + 7;
        IF (corpos <= countchar) THEN
            FOR i IN corpos..countchar LOOP
                fchar := substr(name_r3, i, 1);
                IF (fchar BETWEEN '0' AND '9') THEN
                    comname := comname  fchar;
                END IF;
                IF ((ascii(fchar) >= 65 AND ascii(fchar) <= 90) OR
                    (ascii(fchar) >= 97 AND ascii(fchar) <= 122) OR
                    (ascii(fchar) >= 192 AND ascii(fchar) <= 255)) THEN
                    comname := comname  fchar;
                END IF;
                IF ((fchar = ', ' OR fchar = '. ' OR fchar = ' ' OR i = countchar) AND length(comname) > 0) THEN
                    listt[k] := comname;
                    comname := null;
                    k := k + 1;
                END IF;
            END LOOP;
        END IF;
    ELSE
        listt[1] := replace(name_r3, '''', '');
    END IF;
    
    RETURN listt;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getLinkPKS(listTable varchar, year_pks varchar, group_pks varchar, employer_pks varchar, blink varchar) RETURNS SETOF recPKS AS $$
DECLARE
    rec recPKS;
    employer_t varchar(60) := null;
    mslink_t integer := 0;
    name_t varchar(4000) := null;
    type_t varchar(25) := null;
    powers_t numeric(10,3) := 0;
    fieldoil_t varchar(100) := null;
    yearpks_t varchar(100) := null;
    sql_select varchar(2000);
    sql_add varchar(500) := null;
    table_name varchar(100);
    lTable varchar[];
    listName varchar[];
    first_char varchar(1);
    it integer := 1;
    kt integer := 1;
    rowfound integer := 0;
    r3proc integer;
    cLink integer := 0;
    countlink integer := 0;
    tablecode integer := 0;
    tablenamefromlink varchar(60);
    cur1 refcursor;
BEGIN
    WHILE (length(listTable) > it) LOOP
        table_name := rtrim(substring(listTable from it for position(',' in substring(listTable from it))-1), ',');
        lTable[kt] := table_name;
        it := position(',' in listTable, it) + 1;
        kt := kt + 1;
    END LOOP;
    
    r3proc := 100;
    
    FOR recr3 IN 
        SELECT DISTINCT r3p.enterp_name, r3p.field_name, r3ps.name, r3ps.pks_year, r3ps.po_pks, r3ps.id_sub,
                        r3ps.measureunit, r3ps.pks_power, r3ps.pks_group_name, r3ps.pks_level, pg3.layer_id, pg3.keyid,
                        pksstat.sub_id status_subid, pksstat.status_id
        FROM gis_links.r3_pks r3p
        LEFT JOIN gis_links.r3_pks_sub r3ps ON r3p.id_contractor = r3ps.id_contractor AND r3p.id_cypher = r3ps.id_cypher
        LEFT JOIN gis_links.r3_pks_gis pg3 ON r3ps.id_sub = pg3.r3_mesto
        LEFT JOIN gis_links.r3_pks_statuslink pksstat ON pksstat.sub_id = r3ps.id_sub
        WHERE r3p.enterp_name = employer_pks AND r3ps.pks_year = year_pks AND r3ps.pks_group_name = group_pks
    LOOP
        IF (blink = '0' OR recr3.layer_id IS NULL) THEN
            IF (blink = '0' AND recr3.layer_id IS NOT NULL) THEN
                SELECT t.views INTO tablenamefromlink FROM geo.tables t WHERE t.mslink = recr3.layer_id;
                IF (tablenamefromlink IS NOT NULL) THEN
                    IF (lower(tablenamefromlink) = 'geo.пкс_лэп') THEN
                        sql_select := 'SELECT название,
                                            мощность,
                                            mslink,
                                            NULL тип,
                                            месторождение
                                        FROM '  tablenamefromlink  '

WHERE mslink = '  recr3.keyid;
                    ELSE
                        sql_select := 'SELECT название,
                                            мощность,
                                            mslink,
                                            тип,
                                            месторождение
                                        FROM '  tablenamefromlink  '
                                        WHERE mslink = '  recr3.keyid;
                    END IF;
                    OPEN cur1 FOR sql_select;
                    FETCH cur1 INTO name_t, powers_t, mslink_t, type_t, fieldoil_t;
                    CLOSE cur1;
                    
                    rec.po_pks := recr3.po_pks;
                    rec.id_sub := recr3.id_sub;
                    rec.name_r3 := recr3.name;
                    rec.unit_r3 := recr3.measureunit;
                    rec.powers_r3 := recr3.pks_power;
                    rec.oilfield_r3 := recr3.field_name;
                    rec.powers_gis := powers_t;
                    IF (type_t = 'куст скважин' OR type_t = 'скважина одиночная' OR type_t = 'скважина разведочная') THEN
                        rec.bush_r3 := name_t;
                    ELSE
                        rec.bush_r3 := null;
                    END IF;
                    rec.name_gis := name_t;
                    rec.oilfield_gis := fieldoil_t;
                    rec.mslink := mslink_t;
                    rec.nametable_gis := tablenamefromlink;
                    rec.checklink := 1;
                    RETURN NEXT rec;
                END IF;
            END IF;
        ELSE
            IF (recr3.status_subid IS NULL OR recr3.status_id = 3) THEN
                IF (recr3.pks_group_name = 'Объ-ты жилья') THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                
                first_char := substr(ltrim(recr3.name),1,1);
                IF (first_char >= '0' AND first_char <= '9') THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                
                IF (ascii(first_char) <= 192 AND ascii(first_char) >= 255) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                
                IF (recr3.pks_group_name IN ('Объ-ты произ-го обслуж-я', 'Нефтепромысловые объекты')) THEN
                    IF (recr3.pks_level != 1) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    
                    IF (strpos(lower(recr3.name), 'реконструк') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    ELSIF (strpos(lower(recr3.name), 'техническ') != 0 AND strpos(lower(recr3.name), 'перевооруж') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                END IF;
                
                IF (recr3.pks_group_name IN ('Энергоснабжение', 'Труб-ы, обуст-во кустов', 'Авто-ые дороги и мосты')) THEN
                    IF (recr3.pks_level > 2) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    
                    IF (recr3.pks_group_name = 'Авто-ые дороги и мосты' AND strpos(lower(recr3.name), 'внутрипромысл') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    
                    IF (recr3.pks_group_name = 'Труб-ы, обуст-во кустов' AND strpos(lower(recr3.name), 'площадка') != 0 AND strpos(lower(recr3.name), 'кустовая') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    
                    IF (strpos(lower(recr3.name), 'обустройство') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;

IF (strpos(lower(recr3.name), 'благоустройство') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                    
                    IF (strpos(lower(recr3.name), 'перевод') != 0) THEN
                        r3proc := 0;
                        cLink := 2;
                    END IF;
                END IF;
                
                IF (strpos(lower(recr3.name), 'шурф') != 0 AND strpos(lower(recr3.name), 'куст') != 0) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                
                IF (strpos(lower(recr3.name), 'обустройство') != 0 AND strpos(lower(recr3.name), 'шурф') != 0) THEN
                    r3proc := 0;
                    cLink := 2;
                END IF;
                
                IF (recr3.status_id = 3) THEN
                    cLink := 3;
                END IF;
                
                IF (r3proc = 100) THEN
                    listName := analizestringname(recr3.name);
                    IF array_length(listName, 1) > 0 THEN
                        FOREACH name_i IN ARRAY listName LOOP
                            FOREACH table_i IN ARRAY lTable LOOP
                                IF (lower(table_i) = 'geo.пкс_лэп') THEN
                                    sql_select := 'SELECT название,
                                                        мощность,
                                                        mslink,
                                                        null тип,
                                                        месторождение
                                                    FROM '  table_i  '
                                                    WHERE название = '''  name_i 
                                                        ''' AND заказчик = '''  recr3.enterp_name 
                                                        ''' AND месторождение = '''  recr3.field_name 
                                                        ''' AND год_пкс = '''  year_pks 
                                                        ''' AND группа_пкс = '''  recr3.pks_group_name  '''';
                                ELSE
                                    sql_select := 'SELECT название,
                                                        мощность,
                                                        mslink,
                                                        тип,
                                                        месторождение
                                                    FROM '  table_i  '
                                                    WHERE название = '''  name_i 
                                                        ''' AND заказчик = '''  recr3.enterp_name 
                                                        ''' AND месторождение = '''  recr3.field_name 
                                                        ''' AND год_пкс = '''  year_pks 
                                                        ''' AND группа_пкс = '''  recr3.pks_group_name  '''';
                                END IF;
                                
                                OPEN cur1 FOR sql_select;
                                FETCH cur1 INTO name_t, powers_t, mslink_t, type_t, fieldoil_t;
                                CLOSE cur1;
                                
                                IF (name_t IS NOT NULL) THEN
                                    rec.po_pks := recr3.po_pks;
                                    rec.id_sub := recr3.id_sub;
                                    rec.name_r3 := recr3.name;
                                    rec.unit_r3 := recr3.measureunit;
                                    rec.powers_r3 := recr3.pks_power;
                                    rec.oilfield_r3 := recr3.field_name;
                                    rec.powers_gis := powers_t;

IF (type_t = 'куст скважин' OR type_t = 'скважина одиночная' OR type_t = 'скважина разведочная') THEN
                                        rec.bush_r3 := name_i;
                                    ELSE
                                        rec.bush_r3 := null;
                                    END IF;
                                    
                                    rec.name_gis := name_t;
                                    rec.oilfield_gis := fieldoil_t;
                                    rec.mslink := mslink_t;
                                    rec.nametable_gis := table_i;
                                    rec.checklink := cLink;
                                    RETURN NEXT rec;
                                END IF;
                            END LOOP;
                        END LOOP;
                    END IF;
                    
                    IF (rowfound = 0) THEN
                        rec.po_pks := recr3.po_pks;
                        rec.id_sub := recr3.id_sub;
                        rec.name_r3 := recr3.name;
                        rec.unit_r3 := recr3.measureunit;
                        rec.powers_r3 := recr3.pks_power;
                        rec.oilfield_r3 := recr3.field_name;
                        rec.powers_gis := null;
                        
                        IF (strpos(lower(recr3.name), 'куст') = 1) THEN
                            rec.bush_r3 := listName[1];
                            FOR j IN 2..array_length(listName, 1) LOOP
                                rec.bush_r3 := rec.bush_r3  ','  listName[j];
                            END LOOP;
                        ELSE
                            rec.bush_r3 := null;
                        END IF;
                        
                        rec.name_gis := null;
                        rec.oilfield_gis := null;
                        rec.mslink := null;
                        rec.nametable_gis := null;
                        rec.checklink := cLink;
                        RETURN NEXT rec;
                    END IF;
                ELSE
                    rec.po_pks := recr3.po_pks;
                    rec.id_sub := recr3.id_sub;
                    rec.name_r3 := recr3.name;
                    rec.unit_r3 := recr3.measureunit;
                    rec.powers_r3 := recr3.pks_power;
                    rec.oilfield_r3 := recr3.field_name;
                    rec.powers_gis := null;
                    rec.bush_r3 := null;
                    rec.name_gis := null;
                    rec.oilfield_gis := null;
                    rec.mslink := null;
                    rec.nametable_gis := null;
                    rec.checklink := cLink;
                    RETURN NEXT rec;
                END IF;
                
                r3proc := 100;
                cLink := 0;
            ELSE
                rec.po_pks := recr3.po_pks;
                rec.id_sub := recr3.id_sub;
                rec.name_r3 := recr3.name;
                rec.unit_r3 := recr3.measureunit;
                rec.powers_r3 := recr3.pks_power;
                rec.oilfield_r3 := recr3.field_name;
                rec.powers_gis := null;
                rec.bush_r3 := null;
                
                IF (strpos(lower(recr3.name), 'куст') = 1) THEN
                    listName := analizestringname(recr3.name);
                    rec.bush_r3 := listName[1];
                    FOR j IN 2..array_length(listName, 1) LOOP
                        rec.bush_r3 := rec.bush_r3  ','  listName[j];
                    END LOOP;
                END IF;
                
                rec.name_gis := null;
                rec.oilfield_gis := null;
                rec.mslink := null;
                rec.nametable_gis := null;
                rec.checklink := recr3.status_id;
                RETURN NEXT rec;
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
