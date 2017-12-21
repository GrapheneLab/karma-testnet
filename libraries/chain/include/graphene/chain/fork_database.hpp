/*
 * Copyright (c) 2015 Cryptonomex, Inc., and contributors.
 *
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once
#include <graphene/chain/protocol/block.hpp>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>

#include <boost/config.hpp> /* keep it first to prevent nasty warns in MSVC */
#include <algorithm>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

#define MEMORY_MAPPED_FILE_SIZE 34359738368
#define DATABASE_FILENAME "./fork.db"
#define INDEXES "indexes"
#define UNLINKED_INDEXES "unlinked_indexes"

using boost::multi_index_container;
using namespace boost::multi_index;
namespace bip=boost::interprocess;

namespace graphene { namespace chain {
   using boost::multi_index_container;
   using namespace boost::multi_index;

   struct fork_item
   {
      fork_item( )
      :num(0),id(0),data( std::move(signed_block( )) ),current_offset(0),perv_offset(0) {}

      fork_item( signed_block d )
      :num(d.block_num()), id(d.id()),data( std::move(d) ),current_offset(0),perv_offset(0) {}

      block_id_type previous_id()const { return data.previous; }

      uint32_t              num;    // initialized in ctor
      /**
       * Used to flag a block as invalid and prevent other blocks from
       * building on top of it.
       */
      bool                  invalid = false;
      block_id_type         id;
      signed_block          data;

      uint32_t              current_offset;
      uint32_t              perv_offset;
   };
   typedef shared_ptr<fork_item> item_ptr;

   /**
    *  As long as blocks are pushed in order the fork
    *  database will maintain a linked tree of all blocks
    *  that branch from the start_block.  The tree will
    *  have a maximum depth of 1024 blocks after which
    *  the database will start lopping off forks.
    *
    *  Every time a block is pushed into the fork DB the
    *  block with the highest block_num will be returned.
    */
   class fork_database
   {
      public:
         typedef vector<fork_item> branch_type;
         /// The maximum number of blocks that may be skipped in an out-of-order push
         const static int MAX_BLOCK_REORDERING = 1024;

         fork_database();
         void reset();

         void                             start_block(signed_block b);
         void                             remove(block_id_type b);
         void                             set_head( const fork_item& h);
         bool                             is_known_block(const block_id_type& id)const;
         fork_item                        fetch_block(const block_id_type& id, bool& block_exist )const;
         vector<fork_item>                fetch_block_by_number(uint32_t n)const;

         /**
          *  @return the new head block ( the longest fork )
          */
         fork_item            push_block(const signed_block& b);
         fork_item            head()const 
         {
             bool b; 
             return fetch_block( _head, b ); 
         }
         void                             pop_block();

         /**
          *  Given two head blocks, return two branches of the fork graph that
          *  end with a common ancestor (same prior block)
          */
         pair< branch_type, branch_type >  fetch_branch_from(block_id_type first,
                                                             block_id_type second)const;

         struct block_id;
         struct block_num;
         struct by_previous;
         typedef multi_index_container<
                fork_item,
                indexed_by<
                hashed_unique<tag<block_id>, member<fork_item, block_id_type, &fork_item::id>, std::hash<fc::ripemd160>>,
                hashed_non_unique<tag<by_previous>, const_mem_fun<fork_item, block_id_type, &fork_item::previous_id>, std::hash<fc::ripemd160>>,
                ordered_non_unique<tag<block_num>, member<fork_item,uint32_t,&fork_item::num>>
                >,
                bip::allocator<fork_item,bip::managed_mapped_file::segment_manager>
            > fork_multi_index_type_mm;

            class index_container
            {
                public:
                    index_container( ) {do_init( );};
                    ~index_container( ) {do_close( );};

                    graphene::chain::fork_database::fork_multi_index_type_mm* get_index( ) const{return m_index;} 
                    graphene::chain::fork_database::fork_multi_index_type_mm* get_unlinked_index( ) const{return m_unlinked_index;} 

                    void clear( )
                    {
                        m_index->clear( );
                        m_unlinked_index->clear( );
                    }    

                    void clear_indexes( ){m_index->clear( );}
                    void clear_unlinked_indexes( ){m_unlinked_index->clear( );}

                private: 
                    void do_init( )
                    {
                        bip::file_mapping::remove( DATABASE_FILENAME );
                        m_memory_maped_file = new bip::managed_mapped_file( bip::open_or_create, DATABASE_FILENAME, MEMORY_MAPPED_FILE_SIZE );
                        m_index = m_memory_maped_file->find_or_construct<graphene::chain::fork_database::fork_multi_index_type_mm>( INDEXES )
                        (
                            graphene::chain::fork_database::fork_multi_index_type_mm::ctor_args_list( ),
                            graphene::chain::fork_database::fork_multi_index_type_mm::allocator_type( m_memory_maped_file->get_segment_manager( ) ) 
                        );

                        m_unlinked_index = m_memory_maped_file->find_or_construct<graphene::chain::fork_database::fork_multi_index_type_mm>( UNLINKED_INDEXES )
                        (
                            graphene::chain::fork_database::fork_multi_index_type_mm::ctor_args_list( ),
                            graphene::chain::fork_database::fork_multi_index_type_mm::allocator_type( m_memory_maped_file->get_segment_manager( ) ) 
                        );
                    }

                    void do_close( )
                    {
                        delete m_memory_maped_file;
                        m_memory_maped_file = NULL;

                        bip::file_mapping::remove( DATABASE_FILENAME );
                    }

                    graphene::chain::fork_database::fork_multi_index_type_mm* m_index;
                    graphene::chain::fork_database::fork_multi_index_type_mm* m_unlinked_index;

                    bip::managed_mapped_file* m_memory_maped_file;
            };

         void set_max_size( uint32_t s );

      private:
         /** @return a pointer to the newly pushed item */
         void _push_block(const item_ptr& b);
         void _push_block(fork_item* b);

         void _push_next(const item_ptr& newly_inserted);

         uint32_t                 _max_size = 1024;
         block_id_type            _head;
         bool                     _empty_head;

         index_container indexes;
   };
} } // graphene::chain


