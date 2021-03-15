/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * MySQL.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of MySQL Connector/C++, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#include <mysql/cdk.h>
#include <json_parser.h>
#include <mysqlx/common.h>

#include "op_impl.h"


using namespace ::mysqlx::impl::common;


/*
  A series of converters used to convert DevAPI index specification given
  as a JSON document to the form expected by x-protocol. First, cdk::JSON
  expression needs to be converted to cdk::Any::Document one. Then some field
  name conversions are required, such as "fields" -> "constaint" and
  "field" -> "member". Also, converters check the index specification and throw
  errors if something is not as expected.
*/

struct JSON_val_conv : public cdk::Converter<JSON_val_conv,
  cdk::JSON_processor,
  cdk::Value_processor>
{
  using string = cdk::string;

  void null()
  {
    if (m_proc)
      m_proc->null();
  }

  void str(const string& s)
  {
    if (m_proc)
      m_proc->str(s);
  }

  void num(uint64_t v)
  {
    if (m_proc)
      m_proc->num(v);
  }

  void num(int64_t v)
  {
    if (!m_proc)
      return;

    /*
      Note: xplugin expects positive values to be reported as unsigned integers.
    */

    if (v < 0)
      m_proc->num(v);
    else
      m_proc->num(uint64_t(v));
  }

  void num(float v)
  {
    if (m_proc)
      m_proc->num(v);
  }

  void num(double v)
  {
    if (m_proc)
      m_proc->num(v);
  }

  void yesno(bool v)
  {
    if (m_proc)
      m_proc->yesno(v);
  }

};


struct Field_conv
  : public cdk::Converter<Field_conv,
    cdk::JSON::Processor::Any_prc,
    cdk::Any::Processor
  >
{
  using string = cdk::string;

  using List_conv = cdk::List_prc_converter<JSON_val_conv>;
  using Any_prc = cdk::JSON::Processor::Any_prc;
  using Doc_prc = Any_prc::Doc_prc;

  Any_prc::Scalar_prc* scalar() override
  {
    assert(false);
    return nullptr;
  }

  List_conv m_list_conv;

  Any_prc::List_prc* arr() override
  {
    assert(false);
    return nullptr;
  }

  struct Field_doc_conv
    : public cdk::Doc_prc_converter<JSON_val_conv>
  {
    using Doc_conv = cdk::Doc_prc_converter<JSON_val_conv>;
    bool m_has_required = false;
    bool m_has_options = false;
    bool m_geojson = false;

    Any_prc* key_val(const string &key) override
    {
      static const std::set<std::string> allowed_keys =
      { "field", "type", "required", "options", "srid", "array" };

      std::string field_name = to_lower(key);

      if (allowed_keys.find(field_name) == allowed_keys.end())
        throw_error("Invalid parameter in index field specification");

      // Do a key name replacement on 2nd level
      if (field_name == "field")
      {
        field_name = "member";
      }
      else if (field_name == "required")
      {
        m_has_required = true;
      }
      else if (field_name == "options" || field_name == "srid")
      {
        m_has_options = true;
      }

      // TODO: enable this when m_geojson is correctly set.

      //if (m_has_options && !m_geojson)
      //  throw_error("Only GEOJSON index component can have \"options\" or \"srid\" parameters");

      return Doc_conv::key_val(field_name);
    }

    void doc_begin() override
    {
      m_has_required = false;
    }

    void doc_end() override
    {
      if(m_proc)
      {
        if (!m_has_required)
        {
          // No "required" in "field"
          m_proc->key_val("required")->scalar()->yesno(m_geojson);
        }
        m_proc->doc_end();
      }
    }
  }
  m_doc_conv;

  Doc_prc* doc() override
  {
    auto *prc = m_proc->doc();
    if (!prc)
      return nullptr;
    m_doc_conv.reset(*prc);
    return &m_doc_conv;
  }
};


struct Field_list_conv
  : public cdk::Converter<Field_list_conv,
  cdk::JSON::Processor::Any_prc::List_prc,
  cdk::Any_list::Processor
  >
{
  Field_conv m_field_conv;

  Element_prc* list_el() override
  {
    auto *prc = m_proc->list_el();
    if (!prc)
      return nullptr;
    m_field_conv.reset(*prc);
    return &m_field_conv;
  }

  void list_begin() override
  {
    m_proc->list_begin();
  }

  void list_end() override
  {
    m_proc->list_end();
  }
};


struct Fields_conv
  : public cdk::Converter<Fields_conv,
  cdk::JSON::Processor::Any_prc,
  cdk::Any::Processor
  >
{
  Field_list_conv  m_arr_conv;
  JSON_val_conv m_scalar_conv;

  List_prc* arr() override
  {
    auto *prc = m_proc->arr();
    if (!prc)
      return nullptr;
    m_arr_conv.reset(*prc);
    return &m_arr_conv;
  }

  Scalar_prc* scalar() override
  {
    auto *prc = m_proc->scalar();
    if (!prc)
      return nullptr;

    m_scalar_conv.reset(*prc);
    return &m_scalar_conv;
  }

  Doc_prc* doc() override
  {
    throw_error("Wrong index specification");
    return nullptr;
  }
};


struct Index_def_conv : public cdk::Converter<Index_def_conv,
  cdk::JSON::Processor,
  cdk::Any::Document::Processor>
{
  Fields_conv m_fields_conv;

  Prc_from::Any_prc* key_val(const string &key)
  {
    static const std::set<std::string> allowed_keys = { "fields", "type" };
    std::string field_name = to_lower(key);

    if (allowed_keys.find(field_name) == allowed_keys.end())
      throw_error("Invalid index parameter");

    // Do a key name replacement on 1st level
    if (field_name == "fields")
    {
      field_name = "constraint";
    }

    auto *aprc = m_proc->key_val(field_name);
    if (!aprc)
      return nullptr;
    m_fields_conv.reset(*aprc);
    return &m_fields_conv;
  }
};


/*
  Index_def class represents index definition given as a JSON document in the
  form expected by the protocol. Object of that class acts as a CDK document
  expression containing index description. This description is generated by
  converting input JSON definition using converters defined above.
*/

struct Index_def
 : cdk::Expr_conv_base<Index_def_conv, cdk::JSON, cdk::Any::Document>
{
  parser::JSON_parser m_parser;

  Index_def(const cdk::string &def)
    : m_parser(def)
  {
    reset(m_parser);
  }
};


/*
  This method reports parameters for the "create_collection_index" admin
  command. It adds index definition to the parameters defined with add_param()
  method.
*/

void Op_idx_create::process(cdk::Any::Document::Processor &prc) const
{
  prc.doc_begin();

  for (auto it : m_map)
  {
    Value_scalar val(it.second);
    val.process_if(prc.key_val(it.first));
  }

  // Remove this later
  safe_prc(prc)->key_val("unique")->scalar()->yesno(false);

  // Report remaining values based on JSON document given by user.

  Index_def idx_def(m_def);
  idx_def.process(prc);

  prc.doc_end();
}

/*
  Collection create/modify json options
*/


struct Collection_options_converter
  : public cdk::Converter< cdk::Doc_prc_converter<JSON_val_conv> >
{
  typedef cdk::Converter< cdk::Doc_prc_converter<JSON_val_conv> >  Base;

  typedef typename Base::Prc_from Prc_from;
  typedef typename Base::Prc_to   Prc_to;
  using Base::m_proc;

  typedef cdk::Any_prc_converter<JSON_val_conv> Any_conv;
  typedef typename Prc_from::Any_prc Any_prc;

  void doc_begin() { m_proc->doc_begin(); }
  void doc_end()   { m_proc->doc_end(); }

  Any_conv m_any_conv;

  Any_prc* key_val(const string &key)
  {
    typename Prc_to::Any_prc *ap;
    if(key == "reuseExisting")
    {
      ap = m_proc->key_val("reuse_existing");
    }
    else {
      ap = m_proc->key_val(key);
    }

    if (!ap)
      return NULL;
    m_any_conv.reset(*ap);
    return &m_any_conv;
  }

};

void Op_create_modify_base::process(cdk::Any::Document::Processor &prc) const
{

  prc.doc_begin();

  for (auto it : m_map)
  {
    Value_scalar val(it.second);
    val.process_if(prc.key_val(it.first));
  }

  if(!m_options.empty())
  {
    const parser::JSON_parser parser(m_options);
    Collection_options_converter conv;
    auto * options =
        m_validation_options ?
          prc.key_val("options")->doc()->key_val("validation")->doc()
        : prc.key_val("options")->doc();
    if(options)
    {
      conv.reset(*options);
      parser.process(conv);
    }
    prc.doc_end();
    return;
  }

  if(!m_validation_level.empty() || !m_validation_schema.empty())
  {
    auto options =safe_prc(prc.key_val("options"))->doc();
    options->doc_begin();

    if (!m_validation_level.empty() || !m_validation_schema.empty())
    {
      //validation
      auto validation = options->key_val("validation")->doc();
      validation->doc_begin();

      if(!m_validation_level.empty())
      {
        validation->key_val("level")->scalar()->str(m_validation_level);
      }

      if(!m_validation_schema.empty())
      {
        const parser::JSON_parser parser(m_validation_schema);
        cdk::Doc_prc_converter<JSON_val_conv> conv;
        auto schema = validation->key_val("schema")->doc();
        if(schema)
        {
          conv.reset(*schema);
          parser.process(conv);
        }
      }

      validation->doc_end();
    }

    options->doc_end();
  }
  prc.doc_end();
  return;

}
