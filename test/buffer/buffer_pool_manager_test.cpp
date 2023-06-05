//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, DeletePage) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(10, disk_manager, 5);

  std::vector<Page *> pages;
  std::vector<page_id_t> page_ids;
  std::vector<std::string> content;

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    pages.push_back(new_page);
    page_ids.push_back(temp_page_id);
    content.push_back(std::to_string(i));
  }

  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
  }

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
  }

  auto *new_page = bpm->NewPage(&temp_page_id);
  ASSERT_EQ(nullptr, new_page);

  ASSERT_EQ(0, bpm->DeletePage(page_ids[4]));
  bpm->UnpinPage(4, false);
  bpm->DeletePage(4);
  // ASSERT_EQ(1, bpm->DeletePage(page_ids[4]));

  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  ASSERT_NE(nullptr, new_page);

  auto *page5 = bpm->FetchPage(page_ids[5]);
  ASSERT_NE(nullptr, page5);
  auto *page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page6);
  auto *page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page7);
  strcpy(page5->GetData(), "updatedpage5");  // NOLINT
  strcpy(page6->GetData(), "updatedpage6");  // NOLINT
  strcpy(page7->GetData(), "updatedpage7");  // NOLINT
  bpm->UnpinPage(5, false);
  bpm->UnpinPage(6, false);
  bpm->UnpinPage(7, false);

  bpm->UnpinPage(5, false);
  bpm->UnpinPage(6, false);
  bpm->UnpinPage(7, false);
  ASSERT_EQ(1, bpm->DeletePage(page_ids[7]));

  bpm->NewPage(&temp_page_id);
  page5 = bpm->FetchPage(page_ids[5]);
  page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page5);
  ASSERT_NE(nullptr, page6);
  ASSERT_EQ(0, std::strcmp(page5->GetData(), "updatedpage5"));
  ASSERT_EQ(0, std::strcmp(page6->GetData(), "updatedpage6"));

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, IsDirty) {  // NOLINT
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(1, disk_manager, 5);

  // Make new page and write to it
  page_id_t pageid0;
  auto *page0 = bpm->NewPage(&pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(0, page0->IsDirty());
  strcpy(page0->GetData(), "page0");  // NOLINT
  ASSERT_EQ(1, bpm->UnpinPage(pageid0, true));

  // Fetch again but don't write. Assert it is still marked as dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(1, page0->IsDirty());
  ASSERT_EQ(1, bpm->UnpinPage(pageid0, false));

  // Fetch and assert it is still dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(1, page0->IsDirty());
  ASSERT_EQ(1, bpm->UnpinPage(pageid0, false));

  // Create a new page, assert it's not dirty
  page_id_t pageid1;
  auto *page1 = bpm->NewPage(&pageid1);
  ASSERT_NE(nullptr, page1);
  ASSERT_EQ(0, page1->IsDirty());

  // Write to the page, and then delete it
  strcpy(page1->GetData(), "page1");  // NOLINT
  ASSERT_EQ(1, bpm->UnpinPage(pageid1, true));
  ASSERT_EQ(1, page1->IsDirty());
  ASSERT_EQ(1, bpm->DeletePage(pageid1));

  // Fetch page 0 again, and confirm its not dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(0, page0->IsDirty());

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, ConcurrencyTest) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm]() {  // NOLINT
        page_id_t temp_page_id;
        std::vector<page_id_t> page_ids;
        for (int i = 0; i < 10; i++) {
          auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
          ASSERT_NE(nullptr, new_page);
          strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
          page_ids.push_back(temp_page_id);
        }
        for (int i = 0; i < 10; i++) {
          ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
        }
        for (int j = 0; j < 10; j++) {
          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          ASSERT_NE(nullptr, page);
          ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
          ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], true, nullptr));
        }
        for (int j = 0; j < 10; j++) {
          ASSERT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

}  // namespace bustub
