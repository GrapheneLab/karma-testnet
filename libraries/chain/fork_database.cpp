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
#include <graphene/chain/fork_database.hpp>
#include <graphene/chain/exceptions.hpp>
#include <graphene/chain/protocol/fee_schedule.hpp>
#include <fc/smart_ref_impl.hpp>

namespace graphene { namespace chain {
fork_database::fork_database() : _empty_head( true ), indexes( ) {}

void fork_database::reset()
{
   _empty_head = true;
   indexes.clear( );
}

void fork_database::pop_block()
{
    bool b;
    auto fb = fetch_block( _head, b );
    _head = fb.previous_id( );
    _empty_head = false;
}

void fork_database::start_block(signed_block b)
{
   auto item = std::make_shared<fork_item>(std::move(b));
 
   indexes.get_index( )->insert( fork_item( b ) );

   _head = item->id;
   _empty_head = false;
}

/**
 * Pushes the block into the fork database and caches it if it doesn't link
 *
 */
fork_item  fork_database::push_block(const signed_block& b)
{
   auto item = std::make_shared<fork_item>(b);
   try {
      _push_block(item);
   }
   catch ( const unlinkable_block_exception& e )
   {
      wlog( "Pushing block to fork database that failed to link: ${id}, ${num}", ("id",b.id())("num",b.block_num()) );
      //wlog( "Head: ${num}, ${id}", ("num",_head.data.block_num())("id",_head.data.id()) );
      throw;
      indexes.get_unlinked_index( )->insert( fork_item( b ) );
   }
   bool exist;
   return fetch_block( _head, exist );
}

void  fork_database::_push_block(const item_ptr& item)
{
   if( !_empty_head && item->previous_id() != block_id_type() )
   {
      auto& index = indexes.get_index( )->get<block_id>();
      auto itr = index.find(item->previous_id());
      GRAPHENE_ASSERT(itr != index.end(), unlinkable_block_exception, "block does not link to known chain");
      FC_ASSERT(!(*itr).invalid);
   }

   indexes.get_index( )->insert( *item );

   bool b;
   if( _empty_head )
   { 
       _head = item.get( )->id;
       _empty_head = false;
   }

   else if( item->num > fetch_block( _head, b ).num )
   {
      _head = item.get( )->id;
      _empty_head = false;

      uint32_t min_num = fetch_block( _head, b ).num - std::min( _max_size, fetch_block( _head, b ).num );
      auto& num_idx = indexes.get_index( )->get<block_num>();
      while( num_idx.size() && (*num_idx.begin()).num < min_num )
         num_idx.erase( num_idx.begin() );
      
      indexes.get_unlinked_index( )->get<block_num>().erase(fetch_block( _head, b ).num - _max_size);
   }
   //_push_next( item );
}

void  fork_database::_push_block(fork_item* item)
{
   if( !_empty_head && item->previous_id() != block_id_type() )
   {
      auto& index = indexes.get_index( )->get<block_id>();
      auto itr = index.find(item->previous_id());
      GRAPHENE_ASSERT(itr != index.end(), unlinkable_block_exception, "block does not link to known chain");
      FC_ASSERT(!(*itr).invalid);
   }

   indexes.get_index( )->insert( *item );

   bool b;
   if( _empty_head )
   { 
       _head = item->id;
       _empty_head = false;
   }

   else if( item->num > fetch_block( _head, b ).num )
   {
      _head = item->id;
      _empty_head = false;

      uint32_t min_num = fetch_block( _head, b ).num - std::min( _max_size, fetch_block( _head, b ).num );
      auto& num_idx = indexes.get_index( )->get<block_num>();
      while( num_idx.size() && (*num_idx.begin()).num < min_num )
         num_idx.erase( num_idx.begin() );
      
      indexes.get_unlinked_index( )->get<block_num>().erase(fetch_block( _head, b ).num - _max_size);
   }
}

/**
 *  Iterate through the unlinked cache and insert anything that
 *  links to the newly inserted item.  This will start a recursive
 *  set of calls performing a depth-first insertion of pending blocks as
 *  _push_next(..) calls _push_block(...) which will in turn call _push_next
 */
void fork_database::_push_next( const item_ptr& new_item )
{
    auto& prev_idx = indexes.get_unlinked_index( )->get<by_previous>();

    auto itr = prev_idx.find( new_item->id );
    while( itr != prev_idx.end() )
    {
       auto tmp = *itr;
       prev_idx.erase( itr );
       _push_block( &tmp );

       itr = prev_idx.find( new_item->id );
    }
}

void fork_database::set_max_size( uint32_t s )
{
   _max_size = s;
   if( _empty_head ) return;

   bool b;
   { /// index
      auto& by_num_idx = indexes.get_index( )->get<block_num>();
      auto itr = by_num_idx.begin();
      while( itr != by_num_idx.end() )
      {
         if( (*itr).num < std::max(int64_t(0),int64_t(fetch_block( _head, b ).num) - _max_size) )
            by_num_idx.erase(itr);
         else
            break;
         itr = by_num_idx.begin();
      }
   }
   { /// unlinked_index
      auto& by_num_idx = indexes.get_unlinked_index( )->get<block_num>();
      auto itr = by_num_idx.begin();
      while( itr != by_num_idx.end() )
      {
         if( (*itr).num < std::max(int64_t(0),int64_t(fetch_block( _head, b ).num) - _max_size) )
            by_num_idx.erase(itr);
         else
            break;
         itr = by_num_idx.begin();
      }
   }
}

bool fork_database::is_known_block(const block_id_type& id)const
{
   auto& index = indexes.get_index( )->get<block_id>();
   auto itr = index.find(id);
   if( itr != index.end() )
      return true;

   auto& unlinked_index = indexes.get_unlinked_index( )->get<block_id>();
   auto unlinked_itr = unlinked_index.find(id);
   return unlinked_itr != unlinked_index.end();
}

fork_item fork_database::fetch_block(const block_id_type& id, bool& block_exist )const
{
   block_exist = false;
   auto& index = indexes.get_index( )->get<block_id>();
   auto itr = index.find(id);
   if( itr != index.end( ) )
   {
       block_exist = true;
       return *itr;
   }

   auto& unlinked_index = indexes.get_unlinked_index( )->get<block_id>();
   auto unlinked_itr = unlinked_index.find(id);
   if( unlinked_itr != unlinked_index.end() )
   {
      block_exist = true;
      return fork_item( signed_block( (*unlinked_itr).data ) );
   }
   return fork_item( signed_block( ) );
}

vector<fork_item> fork_database::fetch_block_by_number(uint32_t num)const
{
   vector<fork_item> result;
   auto itr = indexes.get_index( )->get<block_num>().find(num);
   while( itr != indexes.get_index( )->get<block_num>().end( ) )
   {
      if( (*itr).num == num )
         result.push_back( *itr );
      else
         break;
      ++itr;
   }
   return result; 
}

pair<fork_database::branch_type,fork_database::branch_type>
  fork_database::fetch_branch_from(block_id_type first, block_id_type second)const
{ try {
   // This function gets a branch (i.e. vector<fork_item>) leading
   // back to the most recent common ancestor.
   pair<branch_type,branch_type> result;
   auto first_branch_itr = indexes.get_index( )->get<block_id>().find(first);
   FC_ASSERT(first_branch_itr != indexes.get_index( )->get<block_id>().end());
   auto first_branch = *first_branch_itr;

   auto second_branch_itr = indexes.get_index( )->get<block_id>().find(second);
   FC_ASSERT(second_branch_itr != indexes.get_index( )->get<block_id>().end());
   auto second_branch = *second_branch_itr;


   bool first_branch_exist = false;
   bool second_branch_exist = false;   
   while( first_branch.data.block_num() > second_branch.data.block_num() )
   {
      result.first.push_back(first_branch);
      bool b;
      first_branch = fetch_block( first_branch.previous_id( ), b );
      first_branch_exist = true;
   }
   while( second_branch.data.block_num() > first_branch.data.block_num() )
   {
      result.second.push_back( second_branch );
      bool b;
      second_branch = fetch_block( second_branch.previous_id( ), b );
      second_branch_exist = true;
   }
   while( first_branch.data.previous != second_branch.data.previous )
   {
      result.first.push_back(first_branch);
      result.second.push_back(second_branch);
      bool b;
      first_branch = fetch_block( first_branch.previous_id( ), b );
      first_branch_exist = true;
      second_branch = fetch_block( second_branch.previous_id( ), b );
      second_branch_exist = true;
   }
   if( first_branch_exist && second_branch_exist )
   {
      result.first.push_back(first_branch);
      result.second.push_back(second_branch);
   }
   return result;
} FC_CAPTURE_AND_RETHROW( (first)(second) ) }

void fork_database::set_head( const fork_item& h )
{
    _head = h.id;
    _empty_head = false;
}

void fork_database::remove(block_id_type id)
{
   indexes.get_index( )->erase(id);
}

} } // graphene::chain
