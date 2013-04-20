
-- Drop tables etc if they already exist - PostgreSQL
drop sequence if exists common_id_sequence;
drop table if exists departments cascade;

-- Create id sequence
create sequence common_id_sequence;

-- Create tables
create table departments (
  department_id         bigint         not null,
  department_abbr       varchar(100)   not null,
  department_name       varchar(100)   not null unique,
  thingy_type           varchar(100)   not null,
  thingy_number         numeric(10)    not null check (thingy_number >= 42),
  another_thingy        numeric(10,2)  not null check (thingy_number > 0),
  inactivated_datetime  date,
constraint departments_pk primary key (department_id),
constraint departments_department_abbr_bk unique (department_abbr),
constraint departments_thingy_uk unique (thingy_type, thingy_number),
constraint departments_thingy_type_chk
   check (
     thingy_type in ('Strawberry','Chocolate')
   )
);



-- Create foreign keys
--alter table invoices add
-- constraint invoices_debtor_fk foreign key (merchant_abn_number, merchant_debtor_id)
-- references debtors                        (merchant_abn_number, merchant_debtor_id);
