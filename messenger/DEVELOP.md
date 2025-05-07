Applier Code Review

[EVENT]

a. Event Type
	DRC
	Begin
	TableMap
	Rows
		Write
		Update
		Delete
	Xid

b. Example Sequence
	B T W X
	B T U U U X
	B T W D W D X
	B X

c. Event Flow

DumpEventActivity
       |
       V
      GAQ
       |
       V
DispatchEventActivity
       |
       V
    WorkerQ [i]
       |
       V
ApplyEventActivity [i]
       |
       V
      JDBC


[LWM]

GTID:
d10af278-3376-11ea-8417-f9c0d8fa5bd9:42(7, 52)
[sidno]:[gno](last_committed, sequence_number)

a. change of bucket
iff last_committed <= lwm, pass
lwm 3 -> (1, 4) come in -> pass -> commit 4 -> lwm 4

[3, 5, 6, 7, 9] 3 -> commit 4 -> [7, 9] 7

b. special case
sequence number reset:
	 [3, 5, 6, 7, 9] 3 -> (1,2) come in -> [] 0
gap: 
	[7] 7 -> (6, 9) come in -> [8] 8
	[6] 6 (7 doing) -> (6, 9) come in -> [6, 8] 6

[Conflict]

insert() -> removeRowIf() -> updateIf()

update() -> removeRowIf() -> insertIf() -> updateIf()

delete()




