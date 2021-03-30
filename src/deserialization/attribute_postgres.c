#include "attribute_postgres.h"


void
fill_attribute_deserialization_info(AttributeDeserializationInfo * adi, TupleDesc tupledesc, size_t i)
{
	adi->is_dropped = tupledesc->attrs[i]->attisdropped;
	if (adi->is_dropped)
		return;

	Oid			tmp_fn_oid;

	getTypeInputInfo(tupledesc->attrs[i]->atttypid, &tmp_fn_oid, &adi->io_fn_textual.typioparam);
	fmgr_info(tmp_fn_oid, &adi->io_fn_textual.iofunc);
	adi->io_fn_textual.attypmod = tupledesc->attrs[i]->atttypmod;
}
